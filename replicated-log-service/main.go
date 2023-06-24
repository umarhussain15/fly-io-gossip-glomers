package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {

	node := maelstrom.NewNode()

	logs := map[string]map[int64]float64{}

	logsSortedKeys := map[string][]int64{}

	commitOffsets := map[string]int64{}

	//offSets := map[string]int64{}

	var mutexLogs = &sync.RWMutex{}
	var mutexOffsets = &sync.RWMutex{}

	node.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		key := body["key"].(string)
		value := body["msg"].(float64)
		nextOffset := int64(len(logsSortedKeys[key]))

		mutexLogs.Lock()
		if keyLog, ok := logs[key]; ok {
			keyLog[nextOffset] = value
		} else {
			logs[key] = map[int64]float64{
				nextOffset: value,
			}
		}

		if _, ok := logsSortedKeys[key]; ok {
			//len(logsSortedKeys[key])
			logsSortedKeys[key] = append(logsSortedKeys[key], nextOffset)
		} else {
			logsSortedKeys[key] = []int64{
				0,
			}
		}
		mutexLogs.Unlock()

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

		results := map[string][][]int64{}
		mutexLogs.Lock()
		for key, start := range pollOffsets {
			startOffset := int64(start.(float64))
			if _, ok := results[key]; ok {

			} else {
				results[key] = [][]int64{}
			}
			if _, ok := logsSortedKeys[key]; ok {
				if startOffset < int64(len(logsSortedKeys[key])) {
					for i := startOffset; i < startOffset+4 || i < int64(len(logsSortedKeys[key])); i++ {
						results[key] = append(results[key], []int64{
							i,
							int64(logs[key][startOffset]),
						})
					}
				}
			}
		}
		mutexLogs.Unlock()

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

		mutexOffsets.Lock()
		for key, offset := range offsets {
			commitOffsets[key] = int64(offset.(float64))
		}
		mutexOffsets.Unlock()
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
			mutexOffsets.Lock()
			if _, ok := commitOffsets[key.(string)]; ok {
				results[key.(string)] = commitOffsets[key.(string)]
			}
			mutexOffsets.Unlock()
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
