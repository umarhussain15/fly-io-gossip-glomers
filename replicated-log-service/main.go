package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {

	node := maelstrom.NewNode()

	logs := sync.Map{}

	logsSortedKeys := sync.Map{}

	commitOffsets := sync.Map{}

	//offSets := map[string]int64{}

	node.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		key := body["key"].(string)
		value := body["msg"].(float64)
		actual, _ := logsSortedKeys.LoadOrStore(key, []int64{0})
		nextOffset := int64(len(actual.([]int64)))

		//mutexLogs.Lock()
		keyLog := sync.Map{}
		keyLog.Store(nextOffset, value)
		load, ok := logs.LoadOrStore(key, keyLog)
		if ok {
			s := load.(sync.Map)
			s.Store(key, value)
		}

		actual = append(actual.([]int64), nextOffset)

		delete(body, "key")
		delete(body, "msg")
		body["offset"] = float64(nextOffset)
		body["type"] = "send_ok"

		return node.Reply(msg, body)
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		pollOffsets := body["offsets"].(map[string]any)

		results := map[string][][]float64{}
		//mutexLogs.Lock()
		for key, start := range pollOffsets {
			startOffset := start.(float64)
			if _, ok := results[key]; ok {

			} else {
				results[key] = [][]float64{}
			}
			value, ok := logsSortedKeys.Load(key)
			if ok {
				arrLength := float64(len(value.([]int64)))
				keyLogs, ok := logs.Load(key)
				if ok {

					if startOffset < arrLength {
						for i := startOffset; i < startOffset+4 || i < arrLength; i++ {
							s := keyLogs.(sync.Map)
							loaded, ok := s.Load(i)
							if ok {
								results[key] = append(results[key], []float64{
									i,
									loaded.(float64),
								})
							}
						}
					}

				}
			}
		}

		delete(body, "offsets")
		body["msgs"] = results
		body["type"] = "poll_ok"

		return node.Reply(msg, body)
	})

	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		offsets := body["offsets"].(map[string]any)

		for key, offset := range offsets {
			commitOffsets.Store(key, offset)
		}
		delete(body, "offsets")
		body["type"] = "commit_offsets_ok"

		return node.Reply(msg, body)
	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		keys := body["keys"].([]any)
		results := map[string]any{}
		for _, key := range keys {
			value, ok := commitOffsets.Load(key.(string))
			if ok {
				results[key.(string)] = value
			}
		}

		delete(body, "keys")

		body["offsets"] = results
		body["type"] = "list_committed_offsets_ok"

		return node.Reply(msg, body)
	})

	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}
}
