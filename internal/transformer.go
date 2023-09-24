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

func NewTransformer(transformType TransformType, filePath string, outputDir string) Transformer {
	var transformer Transformer

	filename := GetFileName(filePath)
	dirPath := fmt.Sprintf("%s/%s", outputDir, GenerateHash(filename))
	MakeDirectory(dirPath)

	switch transformType {
	case TransformTypeDynamoDB:
		transformer = DynamoDBTransformer{dirPath}
	case TransformTypeParquet:
		transformer = ParquetTransformer{}
	default:
		panic("invalid transform type")
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

	uid := ksuid.New().String()
	file, err := os.Create(fmt.Sprintf("%s/%s.json", dt.dirPath, uid))
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
