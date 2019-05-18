package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
)

// Encrypt the given data with the passphrase
func Encrypt(data []byte, passphrase string) ([]byte, error) {
	gcm, err := getCipher(passphrase)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("error generating nonce for encryption: %v", err)
	}
	return gcm.Seal(nonce, nonce, data, nil), nil
}

// Decrypt the given data using the passphrase
func Decrypt(data []byte, passphrase string) ([]byte, error) {
	gcm, err := getCipher(passphrase)
	if err != nil {
		return nil, err
	}
	nonce, encryptedData := data[:gcm.NonceSize()], data[gcm.NonceSize():]
	return gcm.Open(nil, nonce, encryptedData, nil)
}

func getCipher(passphrase string) (cipher.AEAD, error) {
	// AES requires either 16, 24 or 32 bytes for the key
	// So generate a 32 byte sha256 from the input key and use that with AES
	key := sha256.Sum256([]byte(passphrase))
	c, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	return cipher.NewGCM(c)
}
