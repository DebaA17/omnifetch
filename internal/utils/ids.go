package utils

import (
	"crypto/rand"
	"encoding/hex"
)

func RandHex(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

