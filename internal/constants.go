package internal

import "fmt"

const ProjectLink = "https://github.com/karanpratapsingh/research-csv-etl"

var Description = fmt.Sprintf("ETL tool for large CSV files. For more info checkout %s", ProjectLink)

type Row = []string

type TransformType string

const (
	TransformTypeDynamoDB TransformType = "dynamodb"
	TransformTypeParquet  TransformType = "parquet"
	TransformTypeJSON     TransformType = "json"
	TransformTypeCSV      TransformType = "csv"
)

type ExtensionType string

const (
	ExtensionTypeJSON    ExtensionType = "json"
	ExtensionTypeCSV     ExtensionType = "csv"
	ExtensionTypeParquet ExtensionType = "parquet"
)

var DynamodbTypes = map[string]string{
	"string": "S",
	"number": "N",
	"bool":   "BOOL",
}

var ParquetTypes = map[string]string{
	"string": "type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY",
	"number": "type=INT32",
	"bool":   "type=BOOLEAN",
}
