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

	mu       sync.RWMutex
	messages map[int]int

	topoMu   sync.RWMutex
	topology topologyRequest
}

func NewServer() *server {
	return &server{
		Node: maelstrom.NewNode(),

		mu:       sync.RWMutex{},
		messages: map[int]int{},

		topoMu:   sync.RWMutex{},
		topology: topologyRequest{},
	}
}

func (s *server) start() {
	s.Node.Handle("broadcast", s.broadcastHandler)
	s.Node.Handle("broadcastMessage", s.broadcastMessageHandler)
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
	if _, ok := s.messages[req.Message]; ok {
		s.mu.Unlock()
		return nil
	}
	s.messages[req.Message] = 0
	s.mu.Unlock()
	s.broadCastMessage(msg)

	return s.Node.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}

func (s *server) broadCastMessage(msg maelstrom.Message) error {
	// s.topoMu.RLock()
	// neighborNodes, ok := s.topology.Topology[s.Node.ID()]
	// s.topoMu.RUnlock()
	// if !ok {
	// 	return nil
	// }
	neighborNodes := s.Node.NodeIDs()
	for _, neighbor := range neighborNodes {
		// dont send to the source node or if the neighbor is the source of the message
		if neighbor == s.Node.ID() || neighbor == msg.Src {
			continue
		}
		go func(dest string, msg maelstrom.Message) {
			for {
				if err := s.Node.Send(dest, msg.Body); err != nil {
					// log.Println("Error sending message to neighbor", neighbor, ":", err)
					// time.Sleep(1 * time.Millisecond)
					continue
				}
				break
			}
		}(neighbor, msg)
	}
	return nil
}

func (s *server) broadcastMessageHandler(msg maelstrom.Message) error {
	var req broadcastRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	s.mu.RLock()
	if _, ok := s.messages[req.Message]; !ok {
		s.messages[req.Message] = 0
	}
	s.mu.RUnlock()

	return s.Node.Reply(msg, map[string]any{
		"type": "ok",
	})
}

func (s *server) readHandler(msg maelstrom.Message) error {
	var body map[string]interface{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.RLock()
	var ids []int
	for id := range s.messages {
		ids = append(ids, id)
	}
	s.mu.RUnlock()

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
