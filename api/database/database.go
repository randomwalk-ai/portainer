package database

import (
	"errors"
	"fmt"
	// "os"

	portainer "github.com/portainer/portainer/api"
	"github.com/portainer/portainer/api/database/boltdb"
	"github.com/portainer/portainer/api/database/postgres"
)

var (
	ErrUnknownStoreType = errors.New("unknown database store type")
	ErrEmptyStorePath   = errors.New("store path cannot be empty")
	ErrConnectionFailed = errors.New("failed to establish database connection")
)

// NewDatabase initializes and returns a connection to the specified database.
func NewDatabase(storeType string, storePath string, encryptionKey []byte) (portainer.Connection, error) {
	switch storeType {
	case "boltdb":
		if storePath == "" {
			return nil, ErrEmptyStorePath
		}
		connection := &boltdb.DbConnection{
			Path:          storePath,
			EncryptionKey: encryptionKey,
		}
		return connection, nil

	case "postgres":

		postgresHost := "192.168.218.216"
		postgresPort := "5432"
		postgresUser := "postgres"
		postgresPassword := "s152001s"
		postgresDB := "portainerDB"
		postgresSSLMode := "disable"

		// Validate mandatory fields
		if postgresHost == "" || postgresUser == "" || postgresPassword == "" || postgresDB == "" {
			return nil, errors.New("missing required PostgreSQL environment variables")
		}

		// Build the connection string
		connectionString := fmt.Sprintf(
			"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
			postgresHost,
			defaultIfEmpty(postgresPort, "5432"),  // Default to port 5432 if not specified
			postgresUser,
			postgresPassword,
			postgresDB,
			defaultIfEmpty(postgresSSLMode, "disable"), // Default to "disable" SSL mode
		)

		// Establish the connection
		connection, err := postgres.NewConnection(connectionString, encryptionKey)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrConnectionFailed, err)
		}

		return connection, nil

	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownStoreType, storeType)
	}
}

// defaultIfEmpty returns the fallback value if the input is empty.
func defaultIfEmpty(input, fallback string) string {
	if input == "" {
		return fallback
	}
	return input
}
