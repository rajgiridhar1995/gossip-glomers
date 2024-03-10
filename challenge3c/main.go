package main

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/rs/zerolog"
)

var log zerolog.Logger

func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	tmpFile, err := os.CreateTemp("/Users/rg/go_projects/maelstrom-echo/log", time.Now().Format("2006-01-02T15:04:05"))
	if err != nil {
		os.Exit(1)
	}
	log = zerolog.New(tmpFile).Level(zerolog.DebugLevel).With().Timestamp().Logger()
}

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
	s.Node.Handle("read", s.readHandler)
	s.Node.Handle("topology", s.topologyHandler)

	log.Info().Msg("Starting node")

	if err := s.Node.Run(); err != nil {
		log.Fatal().Err(err).Msg("Error running node")
		// log.Fatal(err)
		os.Exit(1)
	}
	log.Info().Msg("Node exited")
}

func (s *server) broadcastHandler(msg maelstrom.Message) error {
	var req broadcastRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	s.mu.Lock()
	if _, ok := s.messages[req.Message]; ok {
		s.mu.Unlock()
		log.Info().Any("msg", msg).Msg("Message already exists")
		return nil
	}
	s.messages[req.Message] = 0
	s.mu.Unlock()
	log.Info().Any("msg", msg).Msg("Message added to messages")
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
			log.Info().Any("msg", msg).Msgf("Skipping neighbor: %s", neighbor)
			continue
		}
		go func(dest string, msg maelstrom.Message) {
			for {
				v, err := s.Node.SyncRPC(context.Background(), dest, msg.Body)
				if err != nil {
					log.Error().Err(err).Any("msg", msg).Msgf("Error sending message to neighbor: %s. Message: %v", dest, msg)
					// log.Println("Error sending message to neighbor", neighbor, ":", err)
					// time.Sleep(1 * time.Millisecond)
					continue
				}
				if v.Type() != "broadcast_ok" {
					log.Error().Any("msg", msg).Msgf("Error sending message to neighbor")
					// log.Println("Error sending message to neighbor", neighbor, ":", v)
					// time.Sleep(1 * time.Millisecond)
					continue
				}
				log.Info().Any("msg", v).Msgf("Message sent to neighbor: %s", dest)
				break
			}
		}(neighbor, msg)
	}
	return nil
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
	log.Info().Any("msg", msg).Msg("Topology updated")
	return s.Node.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}

func main() {
	s := NewServer()
	s.start()
}
