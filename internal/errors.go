package internal

import (
	"errors"
	"fmt"
)

func ErrFileNotFound(path string) error {
	return fmt.Errorf("file %s not found", path)
}

func ErrReadingFile(err error) error {
	return fmt.Errorf("error reading config file: %v", err)
}

func ErrCountingRows(path string, err error) error {
	return fmt.Errorf("failed to count rows for file %s: %v", path, err)
}

var ErrInsufficientBatchSize = errors.New("batch size cannot be less than 1")

func ErrBatchSizeTooLarge(batchSize, totalPartitions int) error {
	return fmt.Errorf("batch size (%d) should be less than total partitions (%d)", batchSize, totalPartitions)
}

func ErrPartitionSizeTooLarge(partitionSize, totalRows int) error {
	return fmt.Errorf("partition size (%d) should be less than or equal to total number of rows (%d)", partitionSize, totalRows)
}

var ErrSchemaRequired = errors.New("schema definition is required")

func ErrUnsupportedColumnType(columnType string) error {
	return fmt.Errorf("column type %s is not supported", columnType)
}

var ErrDynamoDBTableNotFound = errors.New("table name is required for transform type dynamodb")
var ErrDynamoDBKeyNotFound = errors.New("key is required for transform type dynamodb")

func ErrTransformTypeNotSupported(transformType TransformType) error {
	return fmt.Errorf("transform type %s is not supported", transformType)
}

var ErrEmptySearchPattern = errors.New("search pattern string should not be empty")

func CheckPartitionSize(partitionSize, totalRows int) error {
	if partitionSize > totalRows {
		return ErrPartitionSizeTooLarge(partitionSize, totalRows)
	}
	return nil
}

func CheckBatchSize(batchSize, totalPartitions int) error {
	if batchSize > totalPartitions {
		return ErrBatchSizeTooLarge(batchSize, totalPartitions)
	}
	return nil
}

func CheckTransformType(transformType TransformType, schema Schema) error {
	if transformType == TransformTypeDynamoDB {
		if schema.TableName == "" {
			return ErrDynamoDBTableNotFound
		}

		if schema.Key == "" {
			return ErrDynamoDBKeyNotFound
		}
	}
	return nil
}
