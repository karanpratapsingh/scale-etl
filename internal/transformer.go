package internal

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/segmentio/ksuid"
)

type Transformer interface {
	Transform(records [][]string)
}

func NewTransformer(transformType TransformType, filePath string) Transformer {
	var transformer Transformer

	filename := GetFileName(filePath)
	dirPath := "output/" + GenerateHash(filename) // TODO: convert to sprintf
	MakeDirectory(dirPath)

	switch transformType {
	case TransformTypeDynamoDB:
		transformer = DynamoDBTransformer{dirPath}
	case TransformTypeParquet:
		transformer = ParquetTransformer{}
	}

	fmt.Println("Transform type", transformType)
	return transformer
}

type DynamoDBTransformer struct {
	dirPath string
}

func (dt DynamoDBTransformer) Transform(records [][]string) {
	// TODO: Transform
	jsonData, err := json.Marshal(records)
	if err != nil {
		panic(err)
	}

	file, err := os.Create(dt.dirPath + "/" + ksuid.New().String() + ".json")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	if _, err = file.Write(jsonData); err != nil {
		panic(err)
	}
}

type ParquetTransformer struct{}

func (ParquetTransformer) Transform(records [][]string) {
	fmt.Println("parquet: process", len(records))
}
