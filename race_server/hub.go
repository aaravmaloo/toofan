package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type client struct {
	name   string
	room   string
	ip     string
	events chan []byte
	done   chan struct{}
	closed bool
}

type hub struct {
	mu      sync.Mutex
	rooms   map[string]*room
	clients map[*client]bool
}

func newHub() *hub {
	return &hub{
		rooms:   make(map[string]*room),
		clients: make(map[*client]bool),
	}
}

func (h *hub) onlineCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

func (h *hub) findExistingClient(ip string) *client {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		if c.ip == ip {
			select {
			case <-c.done:
				continue
			default:
				return c
			}
		}
	}
	return nil
}

func (h *hub) findOrCreateRoom(name string, size int, ip string) (*room, *client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	c := &client{
		name:   name,
		ip:     ip,
		events: make(chan []byte, 64),
		done:   make(chan struct{}),
	}
	h.clients[c] = true

	for _, r := range h.rooms {
		if !r.started && len(r.players) < r.maxPlayers && r.maxPlayers == size {
			c.room = r.id
			r.addPlayer(c)
			return r, c
		}
	}

	r := newRoom(h, size)
	h.rooms[r.id] = r
	c.room = r.id
	r.addPlayer(c)
	return r, c
}

func (h *hub) removeClient(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if c.closed {
		return
	}
	c.closed = true
	delete(h.clients, c)
	close(c.done)
	log.Printf("client %s disconnected from %s", c.name, c.ip)

	if r, ok := h.rooms[c.room]; ok {
		r.removePlayer(c)

		r.mu.Lock()
		count := len(r.players)
		started := r.started
		r.mu.Unlock()

		if count == 0 {
			r.close()
			delete(h.rooms, r.id)
		} else {
			if !started {
				r.broadcastLobby()
			} else {
				r.broadcastProgress()
			}
		}
	}
}

func (h *hub) getRoom(id string) *room {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.rooms[id]
}

func (h *hub) serveJoin(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}

	ip := r.Header.Get("X-Forwarded-For")
	if ip == "" {
		ip = r.RemoteAddr
	}
	// Extract the first IP if there is a comma-separated list
	if strings.Contains(ip, ",") {
		ip = strings.Split(ip, ",")[0]
	}
	// Strip port if present
	if strings.Contains(ip, ":") {
		// handle IPv6 edge cases or simple host:port
		lastColon := strings.LastIndex(ip, ":")
		if lastColon > strings.LastIndex(ip, "]") {
			ip = ip[:lastColon]
		}
	}

	if old := h.findExistingClient(ip); old != nil {
		log.Printf("kicking stale session for %s (%s)", old.name, ip)
		h.removeClient(old)
	}

	sizeStr := r.URL.Query().Get("size")
	size := 2
	if sizeStr != "" {
		fmt.Sscanf(sizeStr, "%d", &size)
	}
	if size < 2 {
		size = 2
	} else if size > 10 {
		size = 10
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	rm, c := h.findOrCreateRoom(name, size, ip)
	log.Printf("client %s joined room %s from %s", name, rm.id, ip)

	defer h.removeClient(c)

	joinMsg := marshal(ServerMsg{
		Type: "joined",
		Payload: JoinMsg{
			Room:    rm.id,
			Players: rm.playerNames(),
			Online:  h.onlineCount(),
		},
	})
	fmt.Fprintf(w, "data: %s\n\n", joinMsg)
	flusher.Flush()

	rm.broadcastLobby()

	rm.maybeStart()

	ctx := r.Context()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-ticker.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		case data, ok := <-c.events:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}
