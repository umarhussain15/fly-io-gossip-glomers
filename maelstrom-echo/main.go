package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	node.Handle("echo", func(msg maelstrom.Message) error {
		var body map[string]any

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		body["type"] = "echo_ok"

		return node.Reply(msg, body)
	})

	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}
}
