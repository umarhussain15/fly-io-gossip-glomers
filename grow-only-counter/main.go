package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {

	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)

	keyName := "counter"

	node.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		readInt, err := kv.Read(context.Background(), keyName)
		if err != nil {
			// counter not present in store, default to 0
			readInt = 0
		}
		body["value"] = readInt
		body["type"] = "read_ok"

		return node.Reply(msg, body)
	})

	node.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		delta := body["delta"].(float64)
		var count float64
		err = kv.ReadInto(context.Background(), keyName, &count)
		if err != nil {
			// key not present in store, we set old value as 0 for swap
			count = 0
		}
		err = kv.CompareAndSwap(context.Background(), keyName, count, count+delta, true)
		if err != nil {
			return err
		}
		delete(body, "delta")
		body["type"] = "add_ok"

		return node.Reply(msg, body)
	})

	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}
}
