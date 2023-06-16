package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	var messages []any

	var topology map[string]any

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		messages = append(messages, body["message"])
		if topology != nil {
			// gossip to my neighbors
			for name, secondNeighbors := range topology {
				if name != node.ID() {

					err := node.Send(name, body)
					// failed to send to the neighbor node, we try to propagate message to the neighbors of the neighbor node
					if err != nil {
						for _, neighbor := range secondNeighbors.([]any) {
							if neighbor.(string) != node.ID() {
								err := node.Send(name, body)
								if err != nil {
									return err
								}
							}
						}
					}
				}
			}
		}
		// need to delete from response otherwise maelstrom complains
		delete(body, "message")
		body["type"] = "broadcast_ok"

		return node.Reply(msg, body)
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		body["messages"] = messages
		body["type"] = "read_ok"

		return node.Reply(msg, body)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		topology = body["topology"].(map[string]any)
		delete(body, "topology")
		body["type"] = "topology_ok"

		return node.Reply(msg, body)
	})

	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}
}
