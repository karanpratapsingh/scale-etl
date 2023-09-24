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
		transformer = &DynamoDBTransformer{make(map[string][][]string), sync.Mutex{}}
	case TransformTypeParquet:
		transformer = ParquetTransformer{}
	}

	fmt.Println("Transform type", transformType)
	return transformer
}

type DynamoDBTransformer struct {
	outputs map[string][][]string
	mu      sync.Mutex
}

func (d *DynamoDBTransformer) Transform(id string, records [][]string) {
	d.mu.Lock()
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

		// TODO: test delete from map
		delete(dt.outputs, id)
	}

	var wg sync.WaitGroup

	for id, records := range dt.outputs {
		go func(wg *sync.WaitGroup, id string, records [][]string) {
			defer wg.Done()
			wg.Add(1)

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
