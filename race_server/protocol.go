package main

import "encoding/json"

type ServerMsg struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

type JoinMsg struct {
	Room    string   `json:"room"`
	Players []string `json:"players"`
	Online  int      `json:"online"`
}

type CountdownMsg struct {
	Seconds int `json:"seconds"`
}

type StartMsg struct {
	Text string `json:"text"`
}

type PlayerProgress struct {
	Name     string  `json:"name"`
	Progress float64 `json:"progress"`
	WPM      float64 `json:"wpm"`
	Finished bool    `json:"finished"`
}

type ProgressMsg struct {
	Players []PlayerProgress `json:"players"`
}

type FinishMsg struct {
	Placements []PlayerProgress `json:"placements"`
}

type OnlineMsg struct {
	Count int `json:"count"`
}

type ProgressUpdate struct {
	Name     string  `json:"name"`
	Room     string  `json:"room"`
	Progress float64 `json:"progress"`
	WPM      float64 `json:"wpm"`
}

func marshal(msg ServerMsg) []byte {
	data, _ := json.Marshal(msg)
	return data
}
