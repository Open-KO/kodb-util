package artifacts

import (
	"fmt"
	"kodb-util/config"
	"os"
	"path/filepath"
)

// Artifacts contains reference constants and helpers that map to the OpenKO-db project
// This package shouldn't import any other packages in this project to avoid circular dependencies.
// Exception: config package

const (
	TemplatesDir   = "Templates"
	LoginsDir      = "Logins"
	UsersDir       = "Users"
	SchemasDir     = "Schemas"
	TablesDir      = "Tables"
	ViewsDir       = "Views"
	StoredProcsDir = "StoredProcedures"
	ManualSetupDir = "ManualSetup"

	CreateDatabaseTemplate = "CreateDatabase.sqltemplate"
	CreateUserTemplate     = "CreateUser.sqltemplate"
	CreateLoginTemplate    = "CreateLogin.sqltemplate"
	CreateSchemaTemplate   = "CreateSchema.sqltemplate"

	ExportDatabaseFileNameFmt = "1_CreateDatabase_%s.sql"
	ExportSchemaFileNameFmt   = "2_CreateSchema_%s.sql"
	ExportUserFileNameFmt     = "3_CreateUser_%s.sql"
	ExportLoginFileNameFmt    = "4_CreateLogin_%s.sql"

	SqlExtPattern   = "*.sql"
	BatchTerminator = "\nGO"
)

// ExportDatabaseArtifact writes the generated sql used to create a database in the last import to OpenKO-db/ManualSetup
func ExportDatabaseArtifact(name string, sqlScript string) (err error) {
	return exportManualSetupArtifact(name, sqlScript, ExportDatabaseFileNameFmt)
}

// ExportSchemaArtifact writes the generated sql used to create a schema in the last import to OpenKO-db/ManualSetup
func ExportSchemaArtifact(name string, dbName string, sqlScript string) (err error) {
	// A schema name could exist in multiple databases - prevent collision on filename
	nameFmt := fmt.Sprintf("%s_%s", dbName, name)
	return exportManualSetupArtifact(nameFmt, sqlScript, ExportSchemaFileNameFmt)
}

// ExportUserArtifact writes the generated sql used to create a user in the last import to OpenKO-db/ManualSetup
func ExportUserArtifact(name string, sqlScript string) (err error) {
	return exportManualSetupArtifact(name, sqlScript, ExportUserFileNameFmt)
}

// ExportLoginArtifact writes the generated sql used to create a login in the last import to OpenKO-db/ManualSetup
func ExportLoginArtifact(name string, sqlScript string) (err error) {
	return exportManualSetupArtifact(name, sqlScript, ExportLoginFileNameFmt)
}

func exportManualSetupArtifact(name string, sqlScript string, fileNameFmt string) (err error) {
	fileName := filepath.Join(config.GetConfig().SchemaConfig.Dir, ManualSetupDir, fmt.Sprintf(fileNameFmt, name))
	fmt.Println(fmt.Sprintf("Exporting %s", fileName))
	return os.WriteFile(fileName, []byte(sqlScript), 0644)
}

// GetCreateDatabaseScript loads the CreateDatabase template, substitutes variables, and returns the sql script as a string
func GetCreateDatabaseScript(dbName string) (script string, err error) {
	sqlFmtBytes, err := os.ReadFile(filepath.Join(config.GetConfig().SchemaConfig.Dir, TemplatesDir, CreateDatabaseTemplate))
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(string(sqlFmtBytes), dbName), nil
}

// GetCreateLoginScript loads the CreateLogin template, substitutes variables, and returns the sql script as a string
func GetCreateLoginScript(loginName string, dbName string) (script string, err error) {
	sqlFmtBytes, err := os.ReadFile(filepath.Join(config.GetConfig().SchemaConfig.Dir, TemplatesDir, CreateLoginTemplate))
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(string(sqlFmtBytes), loginName, dbName), nil
}

// GetCreateUserScript loads the CreateUser template, substitutes variables, and returns the sql script as a string
func GetCreateUserScript(userName string, defaultSchema string) (script string, err error) {
	sqlFmtBytes, err := os.ReadFile(filepath.Join(config.GetConfig().SchemaConfig.Dir, TemplatesDir, CreateUserTemplate))
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(string(sqlFmtBytes), userName, defaultSchema), nil
}

// GetCreateSchemaScript loads the CreateSchema template, substitutes variables, and returns the sql script as a string
func GetCreateSchemaScript(schemaName string, dbName string) (script string, err error) {
	sqlFmtBytes, err := os.ReadFile(filepath.Join(config.GetConfig().SchemaConfig.Dir, TemplatesDir, CreateSchemaTemplate))
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(string(sqlFmtBytes), schemaName, dbName), nil
}
