package internal

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
	ExtensionTypeJSON ExtensionType = "json"
	ExtensionTypeCSV  ExtensionType = "csv"
)

var dynamodbTypes = map[string]string{
	"string": "S",
	"number": "N",
	"bool":   "BOOL",
}
