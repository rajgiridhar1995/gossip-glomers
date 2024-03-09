package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastRequest struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type topologyRequest struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type server struct {
	Node *maelstrom.Node

	mu       sync.Mutex
	messages []int

	topoMu   sync.Mutex
	topology topologyRequest
}

func NewServer() *server {
	return &server{
		Node: maelstrom.NewNode(),

		mu:       sync.Mutex{},
		messages: make([]int, 0),

		topoMu:   sync.Mutex{},
		topology: topologyRequest{},
	}
}

func (s *server) start() {
	s.Node.Handle("broadcast", s.broadcastHandler)
	s.Node.Handle("read", s.readHandler)
	s.Node.Handle("topology", s.topologyHandler)

	if err := s.Node.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) broadcastHandler(msg maelstrom.Message) error {
	var req broadcastRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	s.mu.Lock()
	s.messages = append(s.messages, req.Message)
	s.mu.Unlock()

	return s.Node.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}

func (s *server) readHandler(msg maelstrom.Message) error {
	var body map[string]interface{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	var ids []int
	ids = append(ids, s.messages...)
	s.mu.Unlock()

	return s.Node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": ids,
	})
}

func (s *server) topologyHandler(msg maelstrom.Message) error {
	var req topologyRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	s.topoMu.Lock()
	s.topology = req
	s.topoMu.Unlock()
	return s.Node.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}

func main() {
	s := NewServer()
	s.start()
}
