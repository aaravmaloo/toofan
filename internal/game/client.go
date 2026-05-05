package game

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const DefaultServerURL = "http://toofan-race-server.fxtun.dev"

type ServerMsg struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type RacePlayer struct {
	Name     string  `json:"name"`
	Progress float64 `json:"progress"`
	WPM      float64 `json:"wpm"`
	Finished bool    `json:"finished"`
	IsUser   bool    `json:"-"`
}

type LobbyPayload struct {
	Room    string   `json:"room"`
	Players []string `json:"players"`
	Online  int      `json:"online"`
}

type CountdownPayload struct {
	Seconds int `json:"seconds"`
}

type StartPayload struct {
	Text string `json:"text"`
}

type ProgressPayload struct {
	Players []RacePlayer `json:"players"`
}

type FinishPayload struct {
	Placements []RacePlayer `json:"placements"`
}

type OnlinePayload struct {
	Count int `json:"count"`
}

type RaceClient struct {
	serverURL string
	room      string
	name      string
	msgs      chan ServerMsg
	done      chan struct{}
	mu        sync.Mutex
	closed    bool
	client    *http.Client
}

func NewRaceClient(serverURL, username string) *RaceClient {
	if serverURL == "" {
		serverURL = DefaultServerURL
	}
	return &RaceClient{
		serverURL: strings.TrimRight(serverURL, "/"),
		name:      username,
		msgs:      make(chan ServerMsg, 64),
		done:      make(chan struct{}),
		client:    &http.Client{Timeout: 0},
	}
}

func (c *RaceClient) Join(size int) error {
	url := fmt.Sprintf("%s/race/join?name=%s&size=%d", c.serverURL, c.name, size)
	resp, err := c.client.Get(url)
	if err != nil {
		return fmt.Errorf("join failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = fmt.Sprintf("status %d", resp.StatusCode)
		}
		return fmt.Errorf("join failed: %s", msg)
	}

	go c.readSSE(resp.Body)
	return nil
}

func (c *RaceClient) readSSE(body io.ReadCloser) {
	defer body.Close()
	defer close(c.msgs)

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")

		var msg ServerMsg
		if err := json.Unmarshal([]byte(data), &msg); err != nil {
			continue
		}

		select {
		case c.msgs <- msg:
		case <-c.done:
			return
		}
	}
}

func (c *RaceClient) Messages() <-chan ServerMsg {
	return c.msgs
}

func (c *RaceClient) SendProgress(progress float64, wpm float64) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	body := map[string]interface{}{
		"name":     c.name,
		"room":     c.room,
		"progress": progress,
		"wpm":      wpm,
	}
	data, _ := json.Marshal(body)

	ctx := c.client
	resp, err := ctx.Post(c.serverURL+"/race/progress", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (c *RaceClient) SetRoom(room string) {
	c.mu.Lock()
	c.room = room
	c.mu.Unlock()
}

func (c *RaceClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	close(c.done)
}

func (c *RaceClient) Name() string {
	return c.name
}

func PollServerMsg(rc *RaceClient, interval time.Duration) func() ServerMsg {
	return func() ServerMsg {
		select {
		case msg, ok := <-rc.Messages():
			if !ok {
				return ServerMsg{Type: "disconnected"}
			}
			return msg
		case <-time.After(interval):
			return ServerMsg{Type: "tick"}
		}
	}
}
