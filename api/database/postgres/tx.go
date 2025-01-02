package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"context"
	"encoding/binary"

	// "github.com/jmoiron/sqlx"
	dserrors "github.com/portainer/portainer/api/dataservices/errors"

	"github.com/rs/zerolog/log"
)

type DbTransaction struct {
	conn *DbConnection
	tx   *sql.Tx
	ctx  context.Context
}

func (tx *DbTransaction) SetServiceName(bucketName string) error {
    // Create a table that closely mimics BoltDB's key-value storage pattern
    createTableQuery := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            id TEXT PRIMARY KEY,
            data JSONB NOT NULL
        )`, bucketName)
    
    _, err := tx.tx.Exec(createTableQuery)
    return err
}

func (tx *DbTransaction) GetObject(bucketName string, key []byte, object any) error {
    // Determine the key format
    var keyValue any
    keyInt := binary.BigEndian.Uint64(key)
    keyStr := fmt.Sprintf("%d", keyInt)
    
    if bucketName == "settings" || bucketName == "ssl" || bucketName == "version" {
        keyValue = string(key) // Use the key as a string for these buckets
    } else {
        keyValue = keyStr // Default to the integer representation
    }

    // Construct the query
    query := fmt.Sprintf("SELECT data FROM %s WHERE id = $1", bucketName)
    fmt.Println("query:", query)
    fmt.Println("bucketName in getobj:", bucketName)
    fmt.Println("keyValue in getobj:", keyValue)

    // Execute the query
    var rawData []byte
    row := tx.tx.QueryRow(query, keyValue)
    err := row.Scan(&rawData)

    if err != nil {
        fmt.Println("row.Scan error:", err)
        if err == sql.ErrNoRows {
            return fmt.Errorf("%w (bucket=%s, key=%v)", dserrors.ErrObjectNotFound, bucketName, keyValue)
        }
        return err
    }

    // Unmarshal the JSON data into the object
    err = json.Unmarshal(rawData, object)
    if err != nil {
        fmt.Println("json.Unmarshal error:", err)
        return err
    }

    return nil
}


func (tx *DbTransaction) UpdateObject(bucketName string, key []byte, object any) error {
    data, err := json.Marshal(object)
    if err != nil {
        return fmt.Errorf("failed to marshal object: %w", err)
    }

    stringObj := string(data)
    fmt.Println("bucketName in updateobj:", bucketName)
    fmt.Println("key in updateobj:", string(key))
    fmt.Println("stringobj in updateobj:", stringObj)

    var keyValue any
    if bucketName == "settings" || bucketName == "ssl" || bucketName == "version" {
        keyValue = string(key)
    } else {
        keyValue = fmt.Sprintf("%d", binary.BigEndian.Uint64(key))
    }

    // Check if the key exists
    selectQuery := fmt.Sprintf("SELECT id FROM %s WHERE id = $1", bucketName)
    fmt.Println("query in updateobj: ", selectQuery)

    var existingKey string
    err = tx.tx.QueryRow(selectQuery, keyValue).Scan(&existingKey)
    if err == sql.ErrNoRows {
        // Key doesn't exist, insert it
        insertQuery := fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", bucketName)
        _, err = tx.tx.Exec(insertQuery, keyValue, stringObj)
        if err != nil {
            return fmt.Errorf("failed to insert data: %w", err)
        }
    } else if err != nil {
        return fmt.Errorf("failed to query database: %w", err)
    } else {
        // Update the existing key
        updateQuery := fmt.Sprintf("UPDATE %s SET data = $1 WHERE id = $2", bucketName)
        _, err = tx.tx.Exec(updateQuery, stringObj, keyValue)
        if err != nil {
            return fmt.Errorf("failed to execute update query: %w", err)
        }
    }

    return nil
}


func (tx *DbTransaction) DeleteObject(bucketName string, key []byte) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", bucketName)
	keyInt := binary.BigEndian.Uint64(key)
    keyStr := fmt.Sprintf("%d", keyInt)
	_, err := tx.tx.Exec(query, keyStr)
	return err
}

func (tx *DbTransaction) DeleteAllObjects(bucketName string, obj any, matchingFn func(o any) (id int, ok bool)) error {
	// Retrieve all objects
	query := fmt.Sprintf("SELECT id, data FROM %s", bucketName)
	rows, err := tx.tx.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var idsToDelete []int

	// Use reflection to create a new slice of the same type as obj
	objType := reflect.TypeOf(obj)

	for rows.Next() {
		var id string
		var rawData []byte

		if err := rows.Scan(&id, &rawData); err != nil {
			return err
		}

		// Unmarshal the object
		tempObj := reflect.New(objType).Elem()
		if err := json.Unmarshal(rawData, tempObj.Addr().Interface()); err != nil {
			return err
		}

		// Check if the object matches the deletion criteria
		if deleteID, ok := matchingFn(tempObj.Interface()); ok {
			idsToDelete = append(idsToDelete, deleteID)
		}
	}

	// Delete matching objects
	for _, id := range idsToDelete {
		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE id = $1", bucketName)
		_, err := tx.tx.Exec(deleteQuery, id)
		if err != nil {
			return err
		}
	}

	return nil
}

func (tx *DbTransaction) GetNextIdentifier(bucketName string) int {
	var nextID int
	// Convert the max(id) to an integer
	query := fmt.Sprintf("SELECT COALESCE(CAST(MAX(CAST(id AS INTEGER)) AS INTEGER), 0) + 1 FROM %s", bucketName)
	
	err := tx.tx.QueryRow(query).Scan(&nextID)
	if err != nil {
		log.Error().
			Err(err).
			Str("bucket", bucketName).
			Msg("failed to get the next identifier")
		return 0
	}

	return nextID
}

func (tx *DbTransaction) CreateObject(bucketName string, fn func(uint64) (int, any)) error {
	// Get the next sequence number
	var seqID uint64
	// Convert the max(id) to an integer
	query := fmt.Sprintf("SELECT COALESCE(CAST(MAX(CAST(id AS INTEGER)) AS INTEGER), 0) + 1 FROM %s", bucketName)
	err := tx.tx.QueryRow(query).Scan(&seqID)
	if err != nil {
		return fmt.Errorf("failed to fetch next sequence ID: %w", err)
	}

	// Generate the object
	id, obj := fn(seqID)

	// Marshal the object
	data, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("failed to marshal object: %w", err)
	}

	// Insert the object
	insertQuery := fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", bucketName)
	_, err = tx.tx.Exec(insertQuery, id, data)
	if err != nil {
		return fmt.Errorf("failed to insert object into bucket %s: %w", bucketName, err)
	}

	return nil
}

func (tx *DbTransaction) CreateObjectWithId(bucketName string, id int, obj any) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", bucketName)
	_, err = tx.tx.Exec(query, id, data)
	return err
}

func (tx *DbTransaction) CreateObjectWithStringId(bucketName string, id []byte, obj any) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", bucketName)
	_, err = tx.tx.Exec(query, string(id), data)
	return err
}

func (tx *DbTransaction) GetAll(bucketName string, obj any, appendFn func(o any) (any, error)) error {
	query := fmt.Sprintf("SELECT data FROM %s", bucketName)
	rows, err := tx.tx.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var rawData []byte
		if err := rows.Scan(&rawData); err != nil {
			return err
		}

		// Unmarshal the object
		err := json.Unmarshal(rawData, obj)
		if err != nil {
			return err
		}

		// Call the append function
		obj, err = appendFn(obj)
		if err != nil {
			return err
		}
	}

	return nil
}

func (tx *DbTransaction) GetAllWithKeyPrefix(bucketName string, keyPrefix []byte, obj any, appendFn func(o any) (any, error)) error {
	query := fmt.Sprintf("SELECT data FROM %s WHERE id LIKE $1", bucketName)
	rows, err := tx.tx.Query(query, string(keyPrefix)+"%")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var rawData []byte
		if err := rows.Scan(&rawData); err != nil {
			return err
		}

		// Unmarshal the object
		err := json.Unmarshal(rawData, obj)
		if err != nil {
			return err
		}

		// Call the append function
		obj, err = appendFn(obj)
		if err != nil {
			return err
		}
	}

	return nil
}