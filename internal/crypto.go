package internal

import (
	"crypto/sha256"
	"encoding/hex"
)

func generateHash(input string) string {
	hash := sha256.Sum256([]byte(input))
	hashString := hex.EncodeToString(hash[:])

	return hashString[:16]
}
