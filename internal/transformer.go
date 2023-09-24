package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type Transformer interface {
	Transform(id string, records [][]string)
	Save()
}

func NewTransformer(transformType TransformType) Transformer {
	var transformer Transformer

	switch transformType {
	case TransformTypeDynamoDB:
		transformer = &DynamoDBTransformer{sync.Mutex{}, make(map[string][][]string)}
	case TransformTypeParquet:
		transformer = ParquetTransformer{}
	}

	fmt.Println("Transform type", transformType)
	return transformer
}

type DynamoDBTransformer struct {
	mu      sync.Mutex
	outputs map[string][][]string
}

func (d *DynamoDBTransformer) Transform(id string, records [][]string) {
	d.mu.Lock()
	// TODO: ransform records
	d.outputs[id] = append(d.outputs[id], records...)
	d.mu.Unlock()
}

func (dt *DynamoDBTransformer) Save() {
	start := time.Now()

	saveToDisk := func(id string, records [][]string) {
		jsonData, err := json.Marshal(records)
		if err != nil {
			panic(err)
		}

		file, err := os.Create("output/" + id + ".json")
		if err != nil {
			panic(err)
		}
		defer file.Close()

		_, err = file.Write(jsonData)
		if err != nil {
			panic(err)
		}

		delete(dt.outputs, id) // TODO: test delete from map
	}

	var wg sync.WaitGroup

	for id, records := range dt.outputs {
		wg.Add(1)

		go func(wg *sync.WaitGroup, id string, records [][]string) {
			defer wg.Done()
			saveToDisk(id, records)
		}(&wg, id, records)
	}

	wg.Wait()

	duration := time.Since(start)
	fmt.Printf("save completed in %s\n", duration)
}

type ParquetTransformer struct{}

func (ParquetTransformer) Transform(id string, records [][]string) {
	fmt.Println("parquet: process", id, len(records))
}

func (ParquetTransformer) Save() {
	panic("todo")
}
