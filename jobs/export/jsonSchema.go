package export

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kenner2/OpenKO-db/jsonSchema"
	"github.com/kenner2/OpenKO-db/jsonSchema/enums/tsql"
	"kodb-util/config"
	"kodb-util/mssql"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const (
	// jsonSchemaDir is the sub-directory we interact with for exports
	jsonSchemaDir = "jsonSchema"

	// getTableNamesSql pulls a list of all our gameDb table names (dbo schema only) from the INFORMATION_SCHEMA
	getTableNamesSql = `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' and TABLE_TYPE = 'BASE TABLE'`

	// 1. table name
	// jsonSchemaNameFmt output format for jsonSchema file names
	jsonSchemaNameFmt = "%s.json"

	// jsonSchemaSearchPattern is the pattern used to load files from the jsonSchemaDir
	jsonSchemaSearchPattern = "*.json"

	// getColumnDefSqlFmt selects column definition information from the INFORMATION_SCHEMA that we use to sync/create jsonSchema database-based properties
	getColumnDefSqlFmt = `SELECT
	cols.COLUMN_NAME,
	cols.ORDINAL_POSITION,
	cols.COLUMN_DEFAULT,
	cols.IS_NULLABLE,
	cols.DATA_TYPE,
	cols.CHARACTER_MAXIMUM_LENGTH,
	constraints.CONSTRAINT_NAME
FROM INFORMATION_SCHEMA.COLUMNS as cols
LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE as constraints 
	ON constraints.TABLE_SCHEMA = cols.TABLE_SCHEMA and
		constraints.TABLE_NAME = cols.TABLE_NAME and
		constraints.COLUMN_NAME = cols.COLUMN_NAME
where 
	cols.TABLE_SCHEMA = 'dbo' and 
	cols.TABLE_NAME = '%s'
ORDER BY ORDINAL_POSITION`

	// todoMarker is stubbed into new jsonSchema definitions that will need to have codegen-specific properties manually set
	todoMarker = "MANUAL_TODO"
)

// DbColumnDef binds to the result of the getColumnDefSqlFmt query, and is used to map this information into the jsonSchema
type DbColumnDef struct {
	Name       string        `gorm:"column:COLUMN_NAME"`
	Position   int           `gorm:"column:ORDINAL_POSITION"`
	DefaultVal *string       `gorm:"column:COLUMN_DEFAULT"`
	AllowNull  string        `gorm:"column:IS_NULLABLE"`
	Type       tsql.TSqlType `gorm:"column:DATA_TYPE"`
	Length     int           `gorm:"column:CHARACTER_MAXIMUM_LENGTH"`
	Constraint string        `gorm:"column:CONSTRAINT_NAME"`
	//Precision  int                    `gorm:"column:NUMERIC_PRECISION"`
	//Scale      int                    `gorm:"column:NUMERIC_SCALE"`
}

// JsonSchema reads table/column definitions from INFORMATION_SCHEMA and updates/creates jsonSchema definitions with the results
func JsonSchema(driver *mssql.MssqlDbDriver) (err error) {
	fmt.Println("-- Exporting jsonSchema --")

	gormConn, err := driver.GetConnection()
	if err != nil {
		return err
	}
	tableNames := []string{}
	err = gormConn.Raw(getTableNamesSql).Scan(&tableNames).Error
	if err != nil {
		return err
	}
	if len(tableNames) == 0 {
		return fmt.Errorf("no results from INFORMATION_SCHEMA.TABLES")
	}

	jsonSchemaPath := filepath.Join(config.GetConfig().GenConfig.SchemaDir, jsonSchemaDir)
	for i := range tableNames {
		schemaFileName := fmt.Sprintf(jsonSchemaNameFmt, strings.ToLower(tableNames[i]))
		fmt.Println(fmt.Sprintf("Exporting %s to jsonSchema file %s", tableNames[i], schemaFileName))

		// Check if the file already exists
		schemaFilePath := filepath.Join(jsonSchemaPath, schemaFileName)
		fileExists := true
		_, fileErr := os.Stat(schemaFilePath)
		if fileErr != nil {
			if errors.Is(fileErr, os.ErrNotExist) {
				fileExists = false
				fileErr = nil
			} else {
				return fileErr
			}
		}

		jsonTableDef := jsonSchema.TableDef{}
		if fileExists {
			// load existing jsonSchema file to merge data into
			fileBytes, err := os.ReadFile(schemaFilePath)
			if err != nil {
				return err
			}
			err = json.Unmarshal(fileBytes, &jsonTableDef)
			if err != nil {
				return fmt.Errorf("failed to unmarshal into TableDef: %v", err)
			}
		} else {
			// Stub in default information
			jsonTableDef.ClassName = todoMarker
			jsonTableDef.Description = todoMarker
		}

		// make sure name case is in line with database
		jsonTableDef.Name = tableNames[i]

		// update the database type
		jsonTableDef.Database = driver.DbType

		// fetch the column definitions for the table
		var dbColumns []DbColumnDef
		err = gormConn.Raw(fmt.Sprintf(getColumnDefSqlFmt, tableNames[i])).Scan(&dbColumns).Error
		if err != nil {
			return err
		}
		if len(dbColumns) == 0 {
			return fmt.Errorf("no results from INFORMATION_SCHEMA.COLUMNS")
		}

		if jsonTableDef.Columns == nil {
			jsonTableDef.Columns = make([]jsonSchema.Column, 0, len(dbColumns))
		}

		// do a pass removing any non-dbColumns that may exist in jsonTableDef.Columns
		for ix := 0; ix < len(jsonTableDef.Columns); ix++ {
			deletedCol := true
			for jx := range dbColumns {
				if strings.ToLower(jsonTableDef.Columns[ix].Name) == strings.ToLower(dbColumns[jx].Name) {
					deletedCol = false
					break
				}
			}
			if deletedCol {
				fmt.Println(fmt.Sprintf("WARN: Removing column %s from jsonSchema as it is not part of the table definition", jsonTableDef.Columns[ix].Name))
				jsonTableDef.Columns = append(jsonTableDef.Columns[:ix], jsonTableDef.Columns[ix+1:]...)
			}
		}

		// update/add any entries
		for ix := range dbColumns {
			if ix >= len(jsonTableDef.Columns) {
				// insert new entry
				jsonTableDef.Columns = append(jsonTableDef.Columns, getDefaultColumn())
			} else if strings.ToLower(dbColumns[ix].Name) != strings.ToLower(jsonTableDef.Columns[ix].Name) {
				// insert entry at current position
				jsonTableDef.Columns = slices.Insert(jsonTableDef.Columns, ix, getDefaultColumn())
			}
			jsonTableDef.Columns[ix].Name = dbColumns[ix].Name
			jsonTableDef.Columns[ix].Type = dbColumns[ix].Type
			if strings.HasPrefix(dbColumns[ix].Constraint, "PK_") {
				jsonTableDef.Columns[ix].IsPrimaryKey = true
			} else {
				jsonTableDef.Columns[ix].IsPrimaryKey = false
			}

			if strings.HasPrefix(dbColumns[ix].Constraint, "IX_") {
				jsonTableDef.Columns[ix].Unique = dbColumns[ix].Constraint
			} else {
				jsonTableDef.Columns[ix].Unique = ""
			}

			if dbColumns[ix].AllowNull == "YES" {
				jsonTableDef.Columns[ix].AllowNull = true
			} else {
				jsonTableDef.Columns[ix].AllowNull = false
			}
			jsonTableDef.Columns[ix].Type = dbColumns[ix].Type

			jsonTableDef.Columns[ix].DefaultValue = parseDefaultValue(dbColumns[ix].DefaultVal)

			if dbColumns[ix].Length > 8000 {
				// DB using intMax for unspecified length (text/image types, usually)
				dbColumns[ix].Length = 0
			}

			jsonTableDef.Columns[ix].Length = dbColumns[ix].Length

		}

		// sanity check, column list should be in sync
		if len(dbColumns) != len(jsonTableDef.Columns) {
			return fmt.Errorf("dbColumns and jsonTableDef.Columns lengths do not match")
		}

		// write output
		jsonBytes, err := json.MarshalIndent(jsonTableDef, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal jsonTableDef: %v", err)
		}

		// by default the json package writes with LF, but most editors/git uses CRLF
		// convert to CRLF to prevent pointless diffs
		crlfJson := strings.ReplaceAll(string(jsonBytes), "\n", "\r\n")

		err = os.WriteFile(schemaFilePath, []byte(crlfJson), os.ModePerm)
		if err != nil {
			return fmt.Errorf("failed to write jsonTableDef to file: %v", err)
		}
	}

	return nil
}

// parseDefaultValue cleans the parathesis wrapping that sql server adds
func parseDefaultValue(def *string) string {
	if def != nil && len(*def) > 0 {
		origLen := len(*def)
		// remove outer () wraps
		out := strings.TrimLeft(*def, "(")
		leftRemoved := origLen - len(out)
		newLen := len(out) - leftRemoved
		if newLen >= 0 {
			out = out[:newLen]
		} else {
			// our logic doesn't work with whatever mssql gave us
			fmt.Println(fmt.Sprintf("WARN: Unable to unwrap default value %s", *def))
			out = *def
		}
		return out
	}

	return ""
}

// getDefaultColumn returns a column with non-database properties pre-filled with todoMarker
func getDefaultColumn() (col jsonSchema.Column) {
	col.PropertyName = todoMarker
	col.Description = todoMarker
	return col
}
