package postgres

import (
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog/log"
)

// backupMetadata retrieves metadata about tables in the PostgreSQL database
func (c *DbConnection) backupMetadata() (map[string]any, error) {
	query := `
		SELECT 
			table_name, 
			(
				SELECT COUNT(*) 
				FROM information_schema.columns 
				WHERE table_schema = 'public' AND table_name = t.table_name
			) as column_count
		FROM information_schema.tables 
		WHERE table_schema = 'public'
	`

	rows, err := c.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	buckets := make(map[string]any)
	for rows.Next() {
		var tableName string
		var columnCount int
		if err := rows.Scan(&tableName, &columnCount); err != nil {
			return nil, err
		}
		buckets[tableName] = columnCount
	}

	return buckets, nil
}

// ExportJSON creates a JSON representation from the PostgreSQL database
func (c *DbConnection) ExportJSON(metadata bool) ([]byte, error) {
	log.Debug().Msg("Exporting database to JSON")

	backup := make(map[string]any)

	// Export metadata if requested
	if metadata {
		meta, err := c.backupMetadata()
		if err != nil {
			log.Error().Err(err).Msg("failed exporting metadata")
		}
		backup["__metadata"] = meta
	}

	// List of tables to export
	tables := []string{
		"version",
		"ssl",
		"settings", 
		"tunnel_server",
		// Add other tables you want to export
	}

	for _, table := range tables {
		data, err := c.exportTable(table)
		if err != nil {
			log.Error().
				Str("table", table).
				Err(err).
				Msg("failed to export table")
			continue
		}

		// Special handling for specific tables
		switch table {
		case "version", "ssl", "settings", "tunnel_server":
			if len(data) > 0 {
				backup[table] = data[0]
			} else {
				backup[table] = nil
			}
		default:
			if len(data) > 0 {
				backup[table] = data
			}
		}
	}

	return json.MarshalIndent(backup, "", "  ")
}

// exportTable retrieves all rows from a given table
func (c *DbConnection) exportTable(tableName string) ([]any, error) {
	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	
	rows, err := c.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []any

	// Prepare to scan rows
	for rows.Next() {
		// Create a slice of empty interfaces to hold the row data
		rowData := make([]interface{}, len(columns))
		rowPtrs := make([]interface{}, len(columns))
		for i := range columns {
			rowPtrs[i] = &rowData[i]
		}

		// Scan the row
		if err := rows.Scan(rowPtrs...); err != nil {
			return nil, err
		}

		// Convert row to a map
		rowMap := make(map[string]interface{})
		for i, colName := range columns {
			val := rowData[i]
			
			// Handle potential nil values
			if val == nil {
				rowMap[colName] = nil
				continue
			}

			// Special handling for byte slices (potentially encrypted)
			if byteVal, ok := val.([]byte); ok {
				var obj any
				err := c.UnmarshalObject(byteVal, &obj)
				if err == nil {
					rowMap[colName] = obj
				} else {
					// If unmarshaling fails, keep original byte value
					rowMap[colName] = string(byteVal)
				}
			} else {
				rowMap[colName] = val
			}
		}

		results = append(results, rowMap)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}