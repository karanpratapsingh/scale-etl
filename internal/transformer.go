package internal

import (
	"encoding/json"
	"fmt"
	"github.com/segmentio/ksuid"
	"os"
	"sync"
)

type Transformer interface {
	Transform(records [][]string)
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

func (d *DynamoDBTransformer) Transform(records [][]string) {
	// TODO: Transform
	jsonData, err := json.Marshal(records)
	if err != nil {
		panic(err)
	}

	// TODO: auto create folder, hash based on filename
	file, err := os.Create("output/" + ksuid.New().String() + ".json")
	if err != nil {
		panic(err)
	}

	defer file.Close()

	_, err = file.Write(jsonData)
	if err != nil {
		panic(err)
	}
}

type ParquetTransformer struct{}

func (ParquetTransformer) Transform(records [][]string) {
	fmt.Println("parquet: process", len(records))
}
