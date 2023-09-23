package internal

import "fmt"

type Transformer interface {
	Transform(records [][]string)
}

func NewTransformer(transformType TransformType) Transformer {
	var transformer Transformer

	switch transformType {
	case TransformTypeDynamoDB:
		transformer = DynamoDBTransformer{}
	case TransformTypeParquet:
		transformer = ParquetTransformer{}
	}

	fmt.Println("Transform type", transformType)
	return transformer
}

type DynamoDBTransformer struct{}

func (DynamoDBTransformer) Transform(records [][]string) {
	fmt.Println("dynamodb: process", len(records))
}

type ParquetTransformer struct{}

func (ParquetTransformer) Transform(records [][]string) {
	fmt.Println("parquet: process", len(records))
}
