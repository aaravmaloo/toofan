package main

import (
	"sync"
	"time"
)

type player struct {
	client   *client
	name     string
	progress float64
	wpm      float64
	finished bool
}

type room struct {
	id         string
	pin        string
	hub        *hub
	mu         sync.Mutex
	players    map[*client]*player
	maxPlayers int
	started    bool
	counting   bool
	startTime  time.Time
	text       string
	closed     bool
	difficulty string
	mode       string
	lang       string
	duration   int
}

func newRoom(h *hub, id string, pin string, size int, diff, mode, lang string, dur int) *room {
	return &room{
		id:         id,
		pin:        pin,
		hub:        h,
		players:    make(map[*client]*player),
		maxPlayers: size,
		difficulty: diff,
		mode:       mode,
		lang:       lang,
		duration:   dur,
	}
}

func (r *room) addPlayer(c *client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.players[c] = &player{client: c, name: c.name}
}

func (r *room) removePlayer(c *client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.players, c)
}



func (r *room) broadcast(msg ServerMsg) {
	data := marshal(msg)
	r.mu.Lock()
	defer r.mu.Unlock()
	for c := range r.players {
		select {
		case c.events <- data:
		default:
		}
	}
}

func (r *room) broadcastLobby() {
	r.mu.Lock()
	state := "lobby"
	if r.started {
		state = "racing"
	} else if r.counting {
		state = "countdown"
	}

	timeLeft := 0
	if r.started && r.duration > 0 {
		elapsed := time.Since(r.startTime)
		timeLeft = r.duration - int(elapsed.Seconds())
		if timeLeft < 0 {
			timeLeft = 0
		}
	}

	msg := ServerMsg{
		Type: "joined",
		Payload: JoinMsg{
			Room:       r.id,
			Players:    r.playerNamesLocked(),
			Online:     r.hub.onlineCount(),
			Difficulty: r.difficulty,
			Mode:       r.mode,
			Lang:       r.lang,
			Duration:   r.duration,
			IsPrivate:  r.pin != "",
			State:      state,
			Text:       r.text,
			TimeLeft:   timeLeft,
		},
	}
	r.mu.Unlock()
	r.broadcast(msg)
}

func (r *room) playerNamesLocked() []string {
	names := make([]string, 0, len(r.players))
	for _, p := range r.players {
		names = append(names, p.name)
	}
	return names
}

func (r *room) maybeStart() {
	r.mu.Lock()
	count := len(r.players)
	alreadyStarted := r.started
	maxP := r.maxPlayers
	r.mu.Unlock()

	if alreadyStarted || count < maxP {
		return
	}

	go r.startCountdown()
}

func (r *room) startCountdown() {
	r.mu.Lock()
	if r.started || r.counting {
		r.mu.Unlock()
		return
	}
	r.counting = true
	r.mu.Unlock()

	r.broadcast(ServerMsg{
		Type:    "countdown",
		Payload: CountdownMsg{Seconds: 3},
	})

	time.Sleep(3 * time.Second)

	r.mu.Lock()
	r.text = generateText(r.mode, r.lang, r.difficulty)
	r.started = true
	r.counting = false
	r.startTime = time.Now()
	r.mu.Unlock()

	r.broadcast(ServerMsg{
		Type:    "start",
		Payload: StartMsg{
			Text:       r.text,
			Difficulty: r.difficulty,
			Mode:       r.mode,
			Lang:       r.lang,
			Duration:   r.duration,
		},
	})

	if r.duration > 0 {
		go func() {
			time.Sleep(time.Duration(r.duration+5) * time.Second)
			r.forceFinish()
		}()
	}
}

func (r *room) forceFinish() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	
	// Only finish if not already finished by allDone in broadcastProgress
	players := make([]PlayerProgress, 0, len(r.players))
	alreadyFinished := true
	for _, p := range r.players {
		if !p.finished {
			alreadyFinished = false
		}
		players = append(players, PlayerProgress{
			Name:     p.name,
			Progress: p.progress,
			WPM:      p.wpm,
			Finished: p.finished,
		})
	}
	r.mu.Unlock()

	if alreadyFinished {
		return
	}

	// sort by progress desc
	for i := 0; i < len(players); i++ {
		for j := i + 1; j < len(players); j++ {
			if players[j].Progress > players[i].Progress {
				players[i], players[j] = players[j], players[i]
			}
		}
	}

	r.broadcast(ServerMsg{Type: "finish", Payload: FinishMsg{Placements: players}})
}

func (r *room) updateProgress(name string, progress float64, wpm float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, p := range r.players {
		if p.name == name {
			p.progress = progress
			p.wpm = wpm
			if progress >= 1.0 {
				p.finished = true
			}
			break
		}
	}

	r.broadcastProgress()
}

func (r *room) broadcastProgress() {
	r.mu.Lock()
	defer r.mu.Unlock()

	players := make([]PlayerProgress, 0, len(r.players))
	allDone := len(r.players) > 0
	for _, p := range r.players {
		players = append(players, PlayerProgress{
			Name:     p.name,
			Progress: p.progress,
			WPM:      p.wpm,
			Finished: p.finished,
		})
		if !p.finished {
			allDone = false
		}
	}

	// sort by progress desc
	for i := 0; i < len(players); i++ {
		for j := i + 1; j < len(players); j++ {
			if players[j].Progress > players[i].Progress {
				players[i], players[j] = players[j], players[i]
			}
		}
	}

	data := marshal(ServerMsg{Type: "progress", Payload: ProgressMsg{Players: players}})
	for c := range r.players {
		select {
		case c.events <- data:
		default:
		}
	}

	if allDone {
		finishData := marshal(ServerMsg{Type: "finish", Payload: FinishMsg{Placements: players}})
		for c := range r.players {
			select {
			case c.events <- finishData:
			default:
			}
		}
	}
}

func (r *room) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
}
