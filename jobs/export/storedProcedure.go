package export

import (
	"encoding/json"
	"errors"
	"fmt"
	jsonSchema "github.com/Open-KO/kodb-godef"
	"kodb-util/artifacts"
	"kodb-util/config"
	"kodb-util/mssql"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const (
	// getStoredProcedureFmt extracts stored procedures from the database
	getStoredProcedureFmt = `SELECT 
	[name], 
	OBJECT_DEFINITION([object_id]) as [proc],
    [object_id] as [objectId]
FROM [sys].[procedures] 
WHERE [is_ms_shipped] = 0`

	// 1. Stored proc name
	// getProcedureParams returns a list of stored procedure parameter definitions
	getProcedureParams = `SELECT
	[name],  
	type_name([user_type_id]) as [type],  
	[max_length] as [length],
    [parameter_id] as [paramIndex],
    [is_output] as [isOutput]
FROM sys.parameters
WHERE object_id = '%[1]s'`
)

type StoredProcDef struct {
	Name     string `gorm:"column:name"`
	Proc     string `gorm:"column:proc"`
	ObjectId string `gorm:"column:objectId"`
}

func StoredProcedures(driver *mssql.MssqlDbDriver) (err error) {
	fmt.Println("-- Exporting Stored Procedure --")
	// ensure ManualSetup directory exists
	err = os.MkdirAll(filepath.Join(config.GetConfig().GenConfig.SchemaDir, artifacts.ManualSetupDir), os.ModePerm)
	if err != nil {
		fmt.Printf("failed to create the ManualSetup directory: %v\n", err)
		return
	}

	// clean the old export files
	files, err := filepath.Glob(filepath.Join(config.GetConfig().GenConfig.SchemaDir, artifacts.ManualSetupDir, "[8][_]*.sql"))
	if err != nil {
		return err
	}
	for i := range files {
		if err = os.Remove(files[i]); err != nil {
			return err
		}
	}

	gormConn, err := driver.GetConnection()
	if err != nil {
		return err
	}

	// pull the stored procs from the database
	storedProcs := []StoredProcDef{}
	err = gormConn.Raw(getStoredProcedureFmt).Scan(&storedProcs).Error
	if err != nil {
		return err
	}

	procDefs := []jsonSchema.ProcDef{}
	// write them to the output folder
	for i := range storedProcs {
		procDef := jsonSchema.ProcDef{}
		procDef.Name = storedProcs[i].Name
		// get proc params
		var params []jsonSchema.ParamDef
		getParamSql := fmt.Sprintf(getProcedureParams, storedProcs[i].ObjectId)
		err = gormConn.Raw(getParamSql).Scan(&params).Error
		if err != nil {
			return err
		}
		procDef.Params = params
		procDefs = append(procDefs, procDef)

		storedProcs[i].Proc = storedProcs[i].Proc + "\n"
		err = artifacts.ExportStoredProcArtifact(driver, storedProcs[i].Name, storedProcs[i].Proc)
		if err != nil {
			return err
		}
	}

	return updateProcDefs(procDefs)
}

// updateProcDefs exports procedure structure to jsonSchema/procedures
func updateProcDefs(procDefs []jsonSchema.ProcDef) (err error) {
	fmt.Println("-- Exporting procedure jsonSchema --")

	jsonSchemaProcPath := filepath.Join(config.GetConfig().GenConfig.SchemaDir, jsonSchemaDir, jsonSchemaProcedures)
	for i := range procDefs {
		schemaFileName := fmt.Sprintf(jsonSchemaNameFmt, strings.ToLower(procDefs[i].Name))
		fmt.Println(fmt.Sprintf("Exporting %s to procedure json file %s", procDefs[i].Name, schemaFileName))

		// Check if the file already exists
		schemaFilePath := filepath.Join(jsonSchemaProcPath, schemaFileName)
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

		jsonProcDef := jsonSchema.ProcDef{}
		if fileExists {
			// load existing jsonSchema file to merge data into
			fileBytes, err := os.ReadFile(schemaFilePath)
			if err != nil {
				return err
			}
			err = json.Unmarshal(fileBytes, &jsonProcDef)
			if err != nil {
				return fmt.Errorf("failed to unmarshal into TableDef: %v", err)
			}
		} else {
			// Stub in default information
			jsonProcDef.Description = todoMarker
			jsonProcDef.ClassName = snakeToCamelCase(procDefs[i].Name)
		}

		// make sure name case is in line with database
		jsonProcDef.Name = procDefs[i].Name

		if jsonProcDef.Params == nil {
			jsonProcDef.Params = make([]jsonSchema.ParamDef, 0, len(procDefs[i].Params))
		}

		// do a pass removing any non-db params that may exist in jsonProcDef.Params
		for ix := 0; ix < len(jsonProcDef.Params); ix++ {
			deletedParam := true
			for jx := range procDefs[i].Params {
				if strings.ToLower(jsonProcDef.Params[ix].Name) == strings.ToLower(procDefs[i].Params[jx].Name) {
					deletedParam = false
					break
				}
			}
			if deletedParam {
				fmt.Println(fmt.Sprintf("WARN: Removing param %s from jsonSchema as it is not part of the procedure definition", jsonProcDef.Params[ix].Name))
				jsonProcDef.Params = append(jsonProcDef.Params[:ix], jsonProcDef.Params[ix+1:]...)
			}
		}

		// update/add any entries
		for ix := range procDefs[i].Params {
			if ix >= len(jsonProcDef.Params) {
				// insert new entry
				jsonProcDef.Params = append(jsonProcDef.Params, getDefaultParam())
			} else if strings.ToLower(procDefs[i].Params[ix].Name) != strings.ToLower(jsonProcDef.Params[ix].Name) {
				// insert entry at current position
				jsonProcDef.Params = slices.Insert(jsonProcDef.Params, ix, getDefaultParam())
			}
			jsonProcDef.Params[ix].Name = procDefs[i].Params[ix].Name
			jsonProcDef.Params[ix].Type = procDefs[i].Params[ix].Type
			jsonProcDef.Params[ix].Length = procDefs[i].Params[ix].Length
			jsonProcDef.Params[ix].ParamIndex = procDefs[i].Params[ix].ParamIndex
			jsonProcDef.Params[ix].IsOutput = procDefs[i].Params[ix].IsOutput
		}

		// sanity check, column list should be in sync
		if len(procDefs[i].Params) != len(jsonProcDef.Params) {
			return fmt.Errorf("procDefs[i].Params and jsonProcDef.Params lengths do not match")
		}

		// write output
		jsonBytes, err := json.MarshalIndent(jsonProcDef, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal jsonProcDef: %v", err)
		}

		// by default the json package writes with LF, but most editors/git uses CRLF
		// convert to CRLF to prevent pointless diffs
		crlfJson := strings.ReplaceAll(string(jsonBytes), "\n", "\r\n")

		err = os.WriteFile(schemaFilePath, []byte(crlfJson), os.ModePerm)
		if err != nil {
			return fmt.Errorf("failed to write jsonProcDef to file: %v", err)
		}
	}

	return nil
}

// getDefaultParam returns a param with non-database properties pre-filled with todoMarker
func getDefaultParam() (col jsonSchema.ParamDef) {
	col.ParamName = todoMarker
	col.Description = todoMarker
	return col
}

func snakeToCamelCase(dbName string) string {
	tokens := strings.Split(dbName, "_")
	out := strings.Builder{}
	for i := range tokens {
		out.WriteString(strings.Title(strings.ToLower(tokens[i])))
	}
	return out.String()
}
