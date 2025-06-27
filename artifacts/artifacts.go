package artifacts

import (
	"fmt"
	"kodb-util/config"
	"kodb-util/mssql"
	"os"
	"path/filepath"
)

// the artifacts package contains reference constants and helpers that map to the OpenKO-db project
// This package shouldn't import any other packages in this project to avoid circular dependencies.
// Exception: config package

const (

	// directory constants for using the OpenKO-db project
	TemplatesDir   = "Templates"
	ViewsDir       = "Views"
	StoredProcsDir = "StoredProcedures"
	ManualSetupDir = "ManualSetup"

	// template files used to generate several structural exports

	CreateDatabaseTemplate = "CreateDatabase.sqltemplate"
	CreateUserTemplate     = "CreateUser.sqltemplate"
	CreateLoginTemplate    = "CreateLogin.sqltemplate"
	CreateSchemaTemplate   = "CreateSchema.sqltemplate"

	// export file name formats:  [Step]_Create[Type]_[DbType.String()]_[ArtifactName].sql

	ExportDatabaseFileNameFmt        = "1_CreateDatabase_%s.sql"
	ExportSchemaFileNameFmt          = "2_CreateSchema_%s.sql"
	ExportUserFileNameFmt            = "3_CreateUser_%s.sql"
	ExportLoginFileNameFmt           = "4_CreateLogin_%s.sql"
	ExportTableFileNameFmt           = "5_CreateTable_%s.sql"
	ExportTableDataFileNameFmt       = "6_InsertData_%s.sql"
	ExportViewFileNameFmt            = "7_CreateView_%s.sql"
	ExportStoredProcedureFileNameFmt = "8_CreateStoredProc_%s.sql"
)

// ExportDatabaseArtifact writes the generated sql used to create a database in the last import to OpenKO-db/ManualSetup
func ExportDatabaseArtifact(driver *mssql.MssqlDbDriver, sqlScript string) (err error) {
	return exportManualSetupArtifact(driver.GenDbConfig.Name, sqlScript, ExportDatabaseFileNameFmt)
}

// ExportSchemaArtifact writes the generated sql used to create a schema in the last import to OpenKO-db/ManualSetup
func ExportSchemaArtifact(driver *mssql.MssqlDbDriver, schemaIndex int, sqlScript string) (err error) {
	// A schema name could exist in multiple databases - prevent collision on filename
	nameFmt := fmt.Sprintf("%s_%s", driver.GenDbConfig.Name, driver.GenDbConfig.Schemas[schemaIndex])
	return exportManualSetupArtifact(nameFmt, sqlScript, ExportSchemaFileNameFmt)
}

// ExportUserArtifact writes the generated sql used to create a user in the last import to OpenKO-db/ManualSetup
func ExportUserArtifact(driver *mssql.MssqlDbDriver, userIndex int, sqlScript string) (err error) {
	return exportManualSetupArtifact(driver.GenDbConfig.Users[userIndex].Name, sqlScript, ExportUserFileNameFmt)
}

// ExportLoginArtifact writes the generated sql used to create a login in the last import to OpenKO-db/ManualSetup
func ExportLoginArtifact(driver *mssql.MssqlDbDriver, loginIndex int, sqlScript string) (err error) {
	return exportManualSetupArtifact(driver.GenDbConfig.Logins[loginIndex].Name, sqlScript, ExportLoginFileNameFmt)
}

// ExportTableArtifact writes the gorm-generated sql used to create a table in the last import to OpenKO-db/ManualSetup
func ExportTableArtifact(driver *mssql.MssqlDbDriver, name string, sqlScript string) (err error) {
	return exportManualSetupArtifact(name, sqlScript, ExportTableFileNameFmt)
}

// ExportTableDataArtifact writes the gorm-generated sql used to create a table in the last import to OpenKO-db/ManualSetup
func ExportTableDataArtifact(driver *mssql.MssqlDbDriver, name string, sqlScript string) (err error) {
	return exportManualSetupArtifact(name, sqlScript, ExportTableDataFileNameFmt)
}

// ExportStoredProcArtifact writes the sql extracted using a system query to OpenKO-db/ManualSetup
func ExportStoredProcArtifact(driver *mssql.MssqlDbDriver, name string, sqlScript string) (err error) {
	return exportManualSetupArtifact(name, sqlScript, ExportStoredProcedureFileNameFmt)
}

// ExportViewArtifact writes the view sql extracted using a system query to OpenKO-db/ManualSetup
func ExportViewArtifact(driver *mssql.MssqlDbDriver, name string, sqlScript string) (err error) {
	return exportManualSetupArtifact(name, sqlScript, ExportViewFileNameFmt)
}

func exportManualSetupArtifact(name string, sqlScript string, fileNameFmt string) (err error) {
	fileName := filepath.Join(config.GetConfig().GenConfig.SchemaDir, ManualSetupDir, fmt.Sprintf(fileNameFmt, name))
	fmt.Println(fmt.Sprintf("Exporting %s", fileName))
	return os.WriteFile(fileName, []byte(sqlScript), 0644)
}

// GetCreateDatabaseScript loads the CreateDatabase template, substitutes variables, and returns the sql script as a string
func GetCreateDatabaseScript(driver *mssql.MssqlDbDriver) (script string, err error) {
	sqlFmtBytes, err := os.ReadFile(filepath.Join(config.GetConfig().GenConfig.SchemaDir, TemplatesDir, CreateDatabaseTemplate))
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(string(sqlFmtBytes), driver.GenDbConfig.Name), nil
}

// GetCreateLoginScript loads the CreateLogin template, substitutes variables, and returns the sql script as a string
func GetCreateLoginScript(driver *mssql.MssqlDbDriver, loginIndex int) (script string, err error) {
	sqlFmtBytes, err := os.ReadFile(filepath.Join(config.GetConfig().GenConfig.SchemaDir, TemplatesDir, CreateLoginTemplate))
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(string(sqlFmtBytes), driver.GenDbConfig.Logins[loginIndex].Name, driver.GenDbConfig.Name, driver.GenDbConfig.Logins[loginIndex].Pass), nil
}

// GetCreateUserScript loads the CreateUser template, substitutes variables, and returns the sql script as a string
func GetCreateUserScript(driver *mssql.MssqlDbDriver, userIndex int) (script string, err error) {
	sqlFmtBytes, err := os.ReadFile(filepath.Join(config.GetConfig().GenConfig.SchemaDir, TemplatesDir, CreateUserTemplate))
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(string(sqlFmtBytes), driver.GenDbConfig.Users[userIndex].Name, driver.GenDbConfig.Users[userIndex].Schema, driver.GenDbConfig.Name), nil
}

// GetCreateSchemaScript loads the CreateSchema template, substitutes variables, and returns the sql script as a string
func GetCreateSchemaScript(driver *mssql.MssqlDbDriver, schemaIndex int) (script string, err error) {
	sqlFmtBytes, err := os.ReadFile(filepath.Join(config.GetConfig().GenConfig.SchemaDir, TemplatesDir, CreateSchemaTemplate))
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(string(sqlFmtBytes), driver.GenDbConfig.Schemas[schemaIndex], driver.GenDbConfig.Name), nil
}
