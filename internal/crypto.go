package internal

import (
	"crypto/sha256"
	"fmt"
)

func GenerateHash(input string) string {
	hash := sha256.Sum256([]byte(input))
	return fmt.Sprintf("%x", hash)
}
