package export

import (
	"fmt"
	"github.com/Open-KO/OpenKO-gorm/kogen"
	"kodb-util/artifacts"
	"kodb-util/config"
	"kodb-util/mssql"
	"os"
	"path/filepath"
)

// Structure exports structural data from the database into the OpenKO-db/ManualSetup directory;
// these exports include:
// 1_CreateDatabase_[DbType]_*.sql
// 2_CreateSchema_[DbType]_*.sql
// 3_CreateUser_[DbType]_*.sql
// 4_CreateLogin_[DbType]_*.sql
// 5_CreateTable_[DbType]_*.sql
func Structure(driver *mssql.MssqlDbDriver) (err error) {
	fmt.Println("-- Exporting Table Structures --")
	// ensure ManualSetup directory exists
	err = os.MkdirAll(filepath.Join(config.GetConfig().GenConfig.SchemaDir, artifacts.ManualSetupDir), os.ModePerm)
	if err != nil {
		fmt.Printf("failed to create the ManualSetup directory: %v\n", err)
		return
	}

	// clean old artifacts
	files, err := filepath.Glob(filepath.Join(config.GetConfig().GenConfig.SchemaDir, artifacts.ManualSetupDir, "[1-5][_]*.sql"))
	if err != nil {
		return err
	}
	for i := range files {
		if err = os.Remove(files[i]); err != nil {
			return err
		}
	}

	// Export Database as 1_CreateDatabase_%s_*.sql
	script, err := artifacts.GetCreateDatabaseScript(driver)
	if err != nil {
		return err
	}

	err = artifacts.ExportDatabaseArtifact(driver, script)
	if err != nil {
		return err
	}

	// Export Schema as 2_CreateSchema_*.sql
	for i := range driver.GenDbConfig.Schemas {
		script, err = artifacts.GetCreateSchemaScript(driver, i)
		if err != nil {
			return err
		}

		err = artifacts.ExportSchemaArtifact(driver, i, script)
		if err != nil {
			return err
		}
	}

	// Export Users as 3_CreateUser_*.sql
	for i := range driver.GenDbConfig.Users {
		script, err = artifacts.GetCreateUserScript(driver, i)
		if err != nil {
			return err
		}

		err = artifacts.ExportUserArtifact(driver, i, script)
		if err != nil {
			return err
		}
	}

	// Export Logins as 4_CreateLogin_*.sql
	for i := range driver.GenDbConfig.Logins {
		script, err = artifacts.GetCreateLoginScript(driver, i)
		if err != nil {
			return err
		}

		err = artifacts.ExportLoginArtifact(driver, i, script)
		if err != nil {
			return err
		}
	}

	// Export Tables as 5_CreateTable_*.sql
	for i := range kogen.ModelList {
		createTableSql := kogen.ModelList[i].GetCreateTableString()
		err = artifacts.ExportTableArtifact(driver, kogen.ModelList[i].TableName(), createTableSql)
		if err != nil {
			return err
		}
	}

	// TODO: VIEWS/Stored Procedures

	return nil
}
