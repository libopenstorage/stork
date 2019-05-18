// +build unittest

package crypto

import (
	"crypto/rand"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncryptDecrypt(t *testing.T) {
	passphrase := "testkey"
	originalData := make([]byte, 128)

	_, err := io.ReadFull(rand.Reader, originalData)
	require.NoError(t, err, "Error generating test data")

	encryptedData, err := Encrypt(originalData, passphrase)
	require.NoError(t, err, "Error encrypting data")

	decryptedData, err := Decrypt(encryptedData, passphrase)
	require.NoError(t, err, "Error decrypting data")

	require.Equal(t, originalData, decryptedData, "Original and descrypted data mismatch")
}

func TestDecryptInvalidKey(t *testing.T) {
	passphrase := "testkey"
	invalidPassphrase := "invalidKey"
	originalData := make([]byte, 128)

	_, err := io.ReadFull(rand.Reader, originalData)
	require.NoError(t, err, "Error generating test data")

	encryptedData, err := Encrypt(originalData, passphrase)
	require.NoError(t, err, "Error encrypting data")

	decryptedData, err := Decrypt(encryptedData, invalidPassphrase)
	require.Error(t, err, "Decrypting data should have failed")
	require.Nil(t, decryptedData, "Decrypted data should be nil on error")
}

func TestDecryptInvalidiData(t *testing.T) {
	passphrase := "testkey"
	originalData := make([]byte, 128)

	_, err := io.ReadFull(rand.Reader, originalData)
	require.NoError(t, err, "Error generating test data")

	encryptedData, err := Encrypt(originalData, passphrase)
	require.NoError(t, err, "Error encrypting data")

	encryptedData = append(encryptedData, 1)
	decryptedData, err := Decrypt(encryptedData, passphrase)
	require.Error(t, err, "Decrypting data should have failed")
	require.Nil(t, decryptedData, "Decrypted data should be nil on error")
}
