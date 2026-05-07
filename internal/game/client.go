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

const DefaultServerURL = "http://toofan-race.pikapp.in"

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
	Room       string   `json:"room"`
	Players    []string `json:"players"`
	Online     int      `json:"online"`
	Host       string   `json:"host"`
	AutoStart  bool     `json:"auto_start"`
	CanStart   bool     `json:"can_start"`
	Difficulty string   `json:"difficulty"`
	Mode       string   `json:"mode"`
	Lang       string   `json:"lang"`
	Duration   int      `json:"duration"`
	IsPrivate  bool     `json:"is_private"`
	State      string   `json:"state"`
	Text       string   `json:"text,omitempty"`
	TimeLeft   int      `json:"time_left,omitempty"`
}

type CountdownPayload struct {
	Seconds int `json:"seconds"`
}

type StartPayload struct {
	Text       string `json:"text"`
	Difficulty string `json:"difficulty"`
	Mode       string `json:"mode"`
	Lang       string `json:"lang"`
	Duration   int    `json:"duration"`
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

func (c *RaceClient) Join(roomID, pin string, isCreate bool, size int, difficulty, mode, lang string, duration int, autoStart bool) error {
	url := fmt.Sprintf("%s/race/join?name=%s", c.serverURL, c.name)
	if !isCreate && roomID == "" {
		url += "&no_rooms=true"
	}
	if roomID != "" {
		url += "&room=" + roomID
	}
	if pin != "" {
		url += "&pin=" + pin
	}
	if isCreate {
		url += "&is_create=true"
		url += fmt.Sprintf("&size=%d&difficulty=%s&mode=%s&lang=%s&duration=%d&auto_start=%t", size, difficulty, mode, lang, duration, autoStart)
	}

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

	resp, err := c.client.Post(c.serverURL+"/race/progress", "application/json", bytes.NewReader(data))
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

func (c *RaceClient) StartRace() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	room := c.room
	name := c.name
	c.mu.Unlock()

	body := map[string]string{
		"name": name,
		"room": room,
	}
	data, _ := json.Marshal(body)
	resp, err := c.client.Post(c.serverURL+"/race/start", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		msg := strings.TrimSpace(string(b))
		if msg == "" {
			msg = fmt.Sprintf("status %d", resp.StatusCode)
		}
		return fmt.Errorf("start failed: %s", msg)
	}
	return nil
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
