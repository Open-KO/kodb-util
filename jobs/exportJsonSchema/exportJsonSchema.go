package exportJsonSchema

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kenner2/OpenKO-db/jsonSchema"
	"github.com/kenner2/OpenKO-db/jsonSchema/enums/tsql"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"kodb-util/config"
	"kodb-util/mssql"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const (
	jsonSchemaDir           = "jsonSchema"
	getTableNamesSql        = `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' and TABLE_TYPE = 'BASE TABLE'`
	jsonSchemaNameFmt       = "%s.json"
	jsonSchemaSearchPattern = "*.json"

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
)

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

func ExportJsonSchema() (err error) {
	fmt.Println("-- Exporting jsonSchema --")

	driver := mssql.NewMssqlDbDriver()
	gConf := &gorm.Config{}
	dialector := sqlserver.Open(driver.GetConnectionString(config.GetConfig().SchemaConfig.GameDb.Name))
	db, err := gorm.Open(dialector, gConf)
	if err != nil {
		return err
	}
	tableNames := []string{}
	db.Raw(getTableNamesSql).Scan(&tableNames)
	if len(tableNames) == 0 {
		return fmt.Errorf("no results from INFORMATION_SCHEMA.TABLES")
	}

	jsonSchemaPath := filepath.Join(config.GetConfig().SchemaConfig.Dir, jsonSchemaDir)
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
			jsonTableDef.ClassName = "MANUAL_TODO"
			jsonTableDef.Description = "MANUAL_TODO"
			// TODO: When updating for multi-table support, db number will need to be correctly assigned
			jsonTableDef.Database = 0
		}

		// make sure name case is in line with database
		jsonTableDef.Name = tableNames[i]

		// fetch the column definitions for the table
		var dbColumns []DbColumnDef
		db.Raw(fmt.Sprintf(getColumnDefSqlFmt, tableNames[i])).Scan(&dbColumns)
		if len(dbColumns) == 0 {
			return fmt.Errorf("no results from INFORMATION_SCHEMA.COLUMNS")
		}

		if jsonTableDef.Columns == nil {
			jsonTableDef.Columns = make([]jsonSchema.Column, 0, len(dbColumns))
		}

		// do a pass removing any non-db dbColumns
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

			if dbColumns[ix].AllowNull == "YES" {
				jsonTableDef.Columns[ix].AllowNull = true
			} else {
				jsonTableDef.Columns[ix].AllowNull = false
			}
			jsonTableDef.Columns[ix].Type = dbColumns[ix].Type

			jsonTableDef.Columns[ix].DefaultValue = parseDefaultValue(dbColumns[ix].DefaultVal, dbColumns[ix].Type)

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

func parseDefaultValue(def *string, _type tsql.TSqlType) string {
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

// Returns a column with non-database properties pre-filled with TODO Markers
func getDefaultColumn() (col jsonSchema.Column) {
	col.PropertyName = "MANUAL_TODO"
	col.Description = "MANUAL_TODO"
	return col
}
