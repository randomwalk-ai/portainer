package postgres

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"

	"github.com/pkg/errors"
	"github.com/segmentio/encoding/json"
)

var errEncryptedStringTooShort = errors.New("encrypted string too short")

// MarshalObject encodes an object to binary format for PostgreSQL storage
func (connection *DbConnection) MarshalObject(object any) ([]byte, error) {
	buf := &bytes.Buffer{}

	// Special case for VERSION bucket
	if v, ok := object.(string); ok {
		buf.WriteString(v)
	} else {
		enc := json.NewEncoder(buf)
		enc.SetSortMapKeys(false)
		enc.SetAppendNewline(false)

		if err := enc.Encode(object); err != nil {
			return nil, err
		}
	}

	// Check if encryption is enabled
	if connection.getEncryptionKey() == nil {
		return buf.Bytes(), nil
	}

	return encrypt(buf.Bytes(), connection.getEncryptionKey())
}

// UnmarshalObject decodes an object from binary data for PostgreSQL
func (connection *DbConnection) UnmarshalObject(data []byte, object any) error {
	var err error
	
	// Decrypt if encryption key is present
	if connection.getEncryptionKey() != nil {
		data, err = decrypt(data, connection.getEncryptionKey())
		if err != nil {
			return errors.Wrap(err, "Failed decrypting object")
		}
	}

	// Handle JSON unmarshaling
	if e := json.Unmarshal(data, object); e != nil {
		// Special case for VERSION bucket
		s, ok := object.(*string)
		if !ok {
			return errors.Wrap(err, e.Error())
		}

		*s = string(data)
	}

	return err
}

// encrypt performs AES-GCM encryption
func encrypt(plaintext []byte, passphrase []byte) (encrypted []byte, err error) {
	block, err := aes.NewCipher(passphrase)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// decrypt performs AES-GCM decryption
func decrypt(encrypted []byte, passphrase []byte) (plaintextByte []byte, err error) {
	// Special case for "false" string
	if string(encrypted) == "false" {
		return []byte("false"), nil
	}

	block, err := aes.NewCipher(passphrase)
	if err != nil {
		return encrypted, errors.Wrap(err, "Error creating cipher block")
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return encrypted, errors.Wrap(err, "Error creating GCM")
	}

	nonceSize := gcm.NonceSize()
	if len(encrypted) < nonceSize {
		return encrypted, errEncryptedStringTooShort
	}

	nonce, ciphertextByteClean := encrypted[:nonceSize], encrypted[nonceSize:]

	plaintextByte, err = gcm.Open(nil, nonce, ciphertextByteClean, nil)
	if err != nil {
		return encrypted, errors.Wrap(err, "Error decrypting text")
	}

	return plaintextByte, err
}