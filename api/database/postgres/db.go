package postgres

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
	"encoding/json"
	"os"
	"path"

	// "github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	portainer "github.com/portainer/portainer/api"
	dserrors "github.com/portainer/portainer/api/dataservices/errors"
	"github.com/rs/zerolog/log"
)

const (
	// Database configuration constants
	DatabaseDriverName = "postgres"
	DatabaseMaxOpen   = 25
	DatabaseMaxIdle   = 25
	DatabaseTimeout   = 5 * time.Minute

	// Metadata table names
	EncryptedMetadataTable   = "encrypted_metadata"
	UnencryptedMetadataTable = "unencrypted_metadata"
)

const (
	DatabaseFileName          = "portainer"
	EncryptedDatabaseFileName = "portainer"
)

var (
	ErrHaveEncryptedAndUnencrypted = errors.New("portainer has detected both an encrypted and un-encrypted database and cannot start")
	ErrHaveEncryptedWithNoKey      = errors.New("the portainer database is encrypted, but no secret was loaded")
	ErrNoConnection               = errors.New("database connection is not initialized")
)

// DbConnection represents a PostgreSQL database connection
type DbConnection struct {
	ConnectionString string
	Path            string
	EncryptionKey   []byte
	isEncrypted     bool
	ctx             context.Context
	cancelFunc      context.CancelFunc

	DB *sql.DB
}

// NewConnection creates a new database connection
func NewConnection(connectionString string, encryptionKey []byte) (*DbConnection, error) {
	ctx, cancel := context.WithCancel(context.Background())
	
	conn := &DbConnection{
		ConnectionString: connectionString,
		Path:            connectionString,
		EncryptionKey:   encryptionKey,
		ctx:             ctx,
		cancelFunc:      cancel,
	}

	if err := conn.Open(); err != nil {
		cancel()
		return nil, err
	}

	return conn, nil
}

func (connection *DbConnection) GetDatabaseFilePath() string {
	if connection.IsEncryptedStore() {
		return path.Join(connection.Path, EncryptedDatabaseFileName)
	}

	return path.Join(connection.Path, DatabaseFileName)
}

// GetStorePath returns the connection string path
func (connection *DbConnection) GetStorePath() string {
	return connection.Path
}

func (connection *DbConnection) SetEncrypted(flag bool) {
	connection.isEncrypted = flag
}

// IsEncryptedStore returns true if the database is encrypted
func (connection *DbConnection) IsEncryptedStore() bool {
	return connection.getEncryptionKey() != nil
}
func (connection *DbConnection) ExportRaw(filename string) error {
	// Validate that the database file path exists
	databasePath := connection.GetDatabaseFilePath()
	if _, err := os.Stat(databasePath); err != nil {
		return fmt.Errorf("failed to access database file %s: %w", databasePath, err)
	}

	// Export data as JSON
	exportedData, err := connection.ExportDatabaseAsJSON()
	if err != nil {
		return fmt.Errorf("failed to export database to JSON: %w", err)
	}

	// Write the exported data to the specified file
	err = os.WriteFile(filename, exportedData, 0600)
	if err != nil {
		return fmt.Errorf("failed to write data to file %s: %w", filename, err)
	}

	return nil
}
func (connection *DbConnection) ExportDatabaseAsJSON() ([]byte, error) {
	// Query all tables and export their data as JSON
	rows, err := connection.DB.Query(`SELECT tablename FROM pg_tables WHERE schemaname = 'public'`)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	export := make(map[string][]map[string]any)

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}

		// Query table contents
		query := fmt.Sprintf("SELECT * FROM %s", tableName)
		tableRows, err := connection.DB.Query(query)
		if err != nil {
			return nil, fmt.Errorf("failed to query table %s: %w", tableName, err)
		}

		columns, _ := tableRows.Columns()
		for tableRows.Next() {
			values := make([]any, len(columns))
			valuePtrs := make([]any, len(columns))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			err := tableRows.Scan(valuePtrs...)
			if err != nil {
				return nil, fmt.Errorf("failed to scan row for table %s: %w", tableName, err)
			}

			rowData := make(map[string]any)
			for i, col := range columns {
				rowData[col] = values[i]
			}
			export[tableName] = append(export[tableName], rowData)
		}
	}

	return json.MarshalIndent(export, "", "  ")
}

func (connection *DbConnection) ConvertToKey(key int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(key))
    return b
}
// NeedsEncryptionMigration checks if database needs encryption migration
func (connection *DbConnection) NeedsEncryptionMigration() (bool, error) {
	if connection.DB == nil {
		return false, ErrNoConnection
	}

	checkTableExists := func(tableName string) (bool, error) {
		var exists bool
		query := `
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_schema = 'public' 
				AND table_name = $1
			);`
			err := connection.DB.QueryRowContext(connection.ctx, query, tableName).Scan(&exists)
			return exists, err
	}

	haveUnencrypted, err := checkTableExists(UnencryptedMetadataTable)
	if err != nil {
		return false, fmt.Errorf("failed to check unencrypted table: %w", err)
	}

	haveEncrypted, err := checkTableExists(EncryptedMetadataTable)
	if err != nil {
		return false, fmt.Errorf("failed to check encrypted table: %w", err)
	}

	switch {
	case haveUnencrypted && haveEncrypted:
		return false, ErrHaveEncryptedAndUnencrypted
	case haveUnencrypted && connection.EncryptionKey != nil:
		return true, nil
	case haveEncrypted && connection.EncryptionKey == nil:
		return false, ErrHaveEncryptedWithNoKey
	default:
		return false, nil
	}
}

// Open opens and initializes the PostgreSQL database connection
func (connection *DbConnection) Open() error {
	log.Info().Str("connection", connection.ConnectionString).Msg("connecting to PostgreSQL database")

	db, err := sql.Open(DatabaseDriverName, connection.ConnectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("failed to verify database connection: %w", err)
	}
	fmt.Println("Successfully connected to the database!")
	// defer func() {
	// 	if err != nil && db != nil {
	// 		_ = db.Close() // Close the database connection in case of an error
	// 	}
	// }()

	// Create the "users" table if it doesn't exist
	// createTable := `CREATE TABLE IF NOT EXISTS users (
	// 	id SERIAL PRIMARY KEY,
	// 	name VARCHAR(100) NOT NULL,
	// 	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	// )`
	// _, err = db.Exec(createTable)
	// if err != nil {
	// 	return fmt.Errorf("failed to create table: %w", err)
	// }

	// // Insert a test user into the "users" table
	// insertUser := `INSERT INTO users (name) VALUES($1) RETURNING id`
	// var userID int
	// err = db.QueryRow(insertUser, "test").Scan(&userID)
	// if err != nil {
	// 	return fmt.Errorf("failed to insert user: %w", err)
	// }
	// log.Info().Int("user_id", userID).Msg("inserted user")

	db.SetMaxOpenConns(DatabaseMaxOpen)
	db.SetMaxIdleConns(DatabaseMaxIdle)
	db.SetConnMaxLifetime(DatabaseTimeout)

	// Verify connection
	if err := db.PingContext(connection.ctx); err != nil {
		return fmt.Errorf("failed to verify database connection: %w", err)
	}

	connection.DB = db
	return nil
}

// Close closes the PostgreSQL database connection
func (connection *DbConnection) Close() error {
	log.Info().Msg("closing PostgreSQL connection")

	if connection.cancelFunc != nil {
		connection.cancelFunc()
	}

	if connection.DB != nil {
		return connection.DB.Close()
	}

	return nil
}

// UpdateTx executes the given function within a transaction
func (connection *DbConnection) UpdateTx(fn func(portainer.Transaction) error) error {
	// Check if the connection is initialized
	if connection.DB == nil {
		return ErrNoConnection
	}

	// Begin transaction
	tx, err := connection.DB.BeginTx(connection.ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	if tx == nil {
		return fmt.Errorf("transaction object is nil")
	}

	// Ensure transaction cleanup
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			// panic(p) // Re-throw panic after rollback
		}
	}()

	// Wrap transaction in DbTransaction
	pgTx := &DbTransaction{
		conn: connection,
		tx:   tx,
	}

	// Execute the function
	if err := fn(pgTx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error().Err(rbErr).Msg("failed to rollback transaction")
		}
		return fmt.Errorf("transaction function failed: %w", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}



// ViewTx executes a read-only transaction
func (connection *DbConnection) ViewTx(fn func(portainer.Transaction) error) error {
	return connection.UpdateTx(fn) // PostgreSQL doesn't require special handling for read-only transactions
}

// GetNextIdentifier retrieves the next available ID for a table
func (connection *DbConnection) GetNextIdentifier(bucketName string) int {
	var identifier int

	_ = connection.UpdateTx(func(tx portainer.Transaction) error {
		identifier = tx.GetNextIdentifier(bucketName)
		return nil
	})

	return identifier
}

func (connection *DbConnection) GetDatabaseFileName() string {
	if connection.IsEncryptedStore() {
		return EncryptedDatabaseFileName
	}

	return DatabaseFileName
}
func (connection *DbConnection) SetServiceName(bucketName string) error {
	return connection.UpdateTx(func(tx portainer.Transaction) error {
		// Ensure the SetServiceName method exists on the Transaction interface
		return tx.SetServiceName(bucketName)
	})
}
func (connection *DbConnection) UpdateObjectFunc(bucketName string, key []byte, object any, updateFn func()) error {
    // Start a database transaction
    tx, err := connection.DB.Begin()
    if err != nil {
        return fmt.Errorf("failed to begin transaction: %w", err)
    }
    defer func() {
        if err != nil {
            tx.Rollback()
        } else {
            tx.Commit()
        }
    }()

    // Fetch the existing data from the database
    query := fmt.Sprintf("SELECT data FROM %s WHERE id = $1", bucketName)
    var data []byte
    err = tx.QueryRow(query, string(key)).Scan(&data)
    if err == sql.ErrNoRows {
        return fmt.Errorf("%w (bucket=%s, key=%s)", dserrors.ErrObjectNotFound, bucketName, string(key))
    } else if err != nil {
        return fmt.Errorf("failed to retrieve object: %w", err)
    }

    // Unmarshal the data into the provided object
    err = connection.UnmarshalObject(data, object)
    if err != nil {
        return fmt.Errorf("failed to unmarshal object: %w", err)
    }

    // Apply the provided update function to modify the object
    // if err := updateFn(); err != nil {
    //     return fmt.Errorf("update function failed: %w", err)
    // }

    // Marshal the updated object
    updatedData, err := connection.MarshalObject(object)
    if err != nil {
        return fmt.Errorf("failed to marshal object: %w", err)
    }

    // Update the object in the database
    updateQuery := fmt.Sprintf("UPDATE %s SET data = $1 WHERE id = $2", bucketName)
    _, err = tx.Exec(updateQuery, updatedData, string(key))
    if err != nil {
        return fmt.Errorf("failed to update object: %w", err)
    }

    return nil
}


func (connection *DbConnection) GetAllWithKeyPrefix(bucketName string, keyPrefix []byte, obj any, appendFn func(o any) (any, error)) error {
	// Start a transaction view
	return connection.ViewTx(func(tx portainer.Transaction) error {
		// Ensure that the transaction implements the method GetAllWithKeyPrefix
		// Forward the call to the transaction method to handle the database interaction
		return tx.GetAllWithKeyPrefix(bucketName, keyPrefix, obj, appendFn)
	})
}


// BackupTo exports the database to a writer
func (connection *DbConnection) BackupTo(w io.Writer) error {
	if connection.DB == nil {
		return ErrNoConnection
	}

	rows, err := connection.DB.QueryContext(connection.ctx, `
		SELECT 
			table_schema,
			table_name,
			column_name,
			data_type
		FROM 
			information_schema.columns
		WHERE 
			table_schema = 'public'
		ORDER BY 
			table_name, ordinal_position
	`)
	if err != nil {
		return fmt.Errorf("failed to query schema: %w", err)
	}
	defer rows.Close()

	schemas := make(map[string][]string)
	for rows.Next() {
		var schema, table, column, dataType string
		if err := rows.Scan(&schema, &table, &column, &dataType); err != nil {
			return fmt.Errorf("failed to scan schema row: %w", err)
		}
		schemas[table] = append(schemas[table], fmt.Sprintf("%s %s", column, dataType))
	}

	// Write schema information
	for table, columns := range schemas {
		fmt.Fprintf(w, "Table: %s\nColumns:\n", table)
		for _, col := range columns {
			fmt.Fprintf(w, "  %s\n", col)
		}
		fmt.Fprintln(w, "---")
	}

	return nil
}

func (connection *DbConnection) getEncryptionKey() []byte {
	if !connection.isEncrypted {
		return nil
	}
	return connection.EncryptionKey
}

// CreateObject creates a new object in the specified table
// CreateObject creates an object and inserts it with the next ID for the given bucket
func (connection *DbConnection) CreateObject(bucketName string, fn func(uint64) (int, any)) error {
	return connection.UpdateTx(func(tx portainer.Transaction) error {
		return tx.CreateObject(bucketName, fn)
	})
}

// CreateObjectWithId creates a new object in the bucket, using the specified id
func (connection *DbConnection) CreateObjectWithId(bucketName string, id int, obj any) error {
	return connection.UpdateTx(func(tx portainer.Transaction) error {
		return tx.CreateObjectWithId(bucketName, id, obj)
	})
}

// CreateObjectWithStringId creates a new object in the bucket, using the specified id
func (connection *DbConnection) CreateObjectWithStringId(bucketName string, id []byte, obj any) error {
	return connection.UpdateTx(func(tx portainer.Transaction) error {
		return tx.CreateObjectWithStringId(bucketName, id, obj)
	})
}


// MarshalObject converts an object to JSON
// func (connection *DbConnection) MarshalObject(obj any) ([]byte, error) {
// 	return json.Marshal(obj)
// }

// // UnmarshalObject converts JSON to an object
// func (connection *DbConnection) UnmarshalObject(data []byte, obj any) error {
// 	return json.Unmarshal(data, obj)
// }

// Other methods would be implemented similarly...

// GetObject retrieves an object from a table
func (connection *DbConnection) GetObject(bucketName string, key []byte, object any) error {
	return connection.ViewTx(func(tx portainer.Transaction) error {
		return tx.GetObject(bucketName, key, object)
	})
}

// UpdateObject updates an object in a table
func (connection *DbConnection) UpdateObject(bucketName string, key []byte, object any) error {
	return connection.UpdateTx(func(tx portainer.Transaction) error {
		return tx.UpdateObject(bucketName, key, object)
	})
}

// DeleteObject removes an object from a table
func (connection *DbConnection) DeleteObject(bucketName string, key []byte) error {
	return connection.UpdateTx(func(tx portainer.Transaction) error {
		return tx.DeleteObject(bucketName, key)
	})
}

// GetAll retrieves all objects from a table
func (connection *DbConnection) GetAll(bucketName string, obj any, appendFn func(o any) (any, error)) error {
	return connection.ViewTx(func(tx portainer.Transaction) error {
		return tx.GetAll(bucketName, obj, appendFn)
	})
}

// DeleteAllObjects deletes all objects from a specific bucket (table) in the database that match a given condition.
func (connection *DbConnection) DeleteAllObjects(bucketName string, obj any, matching func(o any) (id int, ok bool)) error {
	return connection.UpdateTx(func(tx portainer.Transaction) error {
		return tx.DeleteAllObjects(bucketName, obj, matching)
	})
}

// BackupMetadata retrieves sequence/identity information
func (connection *DbConnection) BackupMetadata() (map[string]any, error) {
	metadata := make(map[string]any)

	// Query to fetch all table names and serial sequences in the public schema
	rows, err := connection.DB.Query(`
		SELECT tablename, pg_get_serial_sequence(tablename, 'id') as seq
		FROM pg_tables 
		WHERE schemaname = 'public'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, seqName string

		// Scan each row for table name and sequence name
		if err := rows.Scan(&tableName, &seqName); err != nil {
			return nil, err
		}

		if seqName != "" {
			var seqValue sql.NullInt64

			// Query to fetch the last value of the sequence
			err := connection.DB.QueryRow(fmt.Sprintf("SELECT last_value FROM %s", seqName)).Scan(&seqValue)
			if err != nil {
				// Log the error and continue processing other tables
				continue
			}

			// Add the sequence value to the metadata map if valid
			if seqValue.Valid {
				metadata[tableName] = seqValue.Int64
			}
		}
	}

	return metadata, nil
}


// RestoreMetadata sets sequence/identity values for tables
func (connection *DbConnection) RestoreMetadata(s map[string]any) error {
	for tableName, v := range s {
		id, ok := v.(float64)
		if !ok {
			log.Error().Str("table", tableName).Msg("failed to restore metadata")
			continue
		}

		seqName := fmt.Sprintf("%s_id_seq", tableName)
		_, err := connection.DB.Exec(fmt.Sprintf("ALTER SEQUENCE %s RESTART WITH %d", seqName, int64(id)))
		if err != nil {
			log.Error().Err(err).Str("table", tableName).Msg("failed to restore sequence")
		}
	}

	return nil
}