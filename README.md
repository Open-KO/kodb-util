# kodb-util
`kodb-util` is a database utility tool created to help in the development of the OpenKO project.

The utility aims to:

1. Create/Import the OpenKO database, users, and stored procedures in the OpenKO-db project.
2. TODO: Update the files in the OpenKO-db project via extract functions.

## Program configuration
Configure your database connection information in `kodb-util-config.yaml`; As this file is on gitignore, a template is provided in `kodb-util-config.yaml.template`

This utility mutates the `knight` user as part of its import and export functionality.  For local development you can:
* Leave databaseConfig.user blank to use Windows Authentication
* use your `sa` login
* configure a user with similar permissions

You'll need a copy of OpenKO-db to run this program against.  In your config file, set schemaConfig.dir to the directory 
that contains this project. 

(TODO:  Link OpenKO-db repo when opened under OpenKO org)

## Dependencies
The following commands assume that you have a terminal open in the root folder of the project.

The `OpenKO-db` project is a submodule; We make use of its templates to generate DBs, Schemas, Users, and Logins.
We can also export table properties from the database to update jsonSchema.

To get the submodule for the first time:
```shell
git submodule update --init --recursive
```

To get updates after the submodule has been pulled for the first time:
```shell
git submodule update --recursive --remote
```

This utility is programmed with Go 1.24+.  You'll need to install the language if you want to build locally. See https://go.dev/doc/install

If Go is correctly installed on your path, you should be able to run `go version` in your terminal and get version
information output:
```
PS C:\> go version
go version go1.24.1 windows/amd64
```
To download Go dependencies, run:
```shell
go mod download
```

To run the application, run:
```shell
go run kodb-util.go
```

Without any arguments, you should get a usage prompt like this:
```
|---------------------------|
| OpenKO Database Utilities |
|---------------------------|
No arguments provided:
Usage of kodb-util.exe:
  -clean
        Clean drops the databaseConfig.dbname database and removes the knight user
  -config string
        Path to config file (default "kodb-util-config.yaml")
  -createManualArtifacts
        Export the artifacts generated from templates during the import process to OpenKO-db/ManualSetup.  Performs a clean on the ManualSetup directory first.
  -dbpass string
        Database password override
  -dbuser string
        Database user override
  -exportJsonSchema
        Export table properties from the database to update jsonSchema
  -import
        Runs clean and imports OpenKO-db files
  -schema string
        OpenKO-db schema directory override
```

## Building the utility program
In order to build `kodb-util.exe`, run the following command in this directory:
```shell
go build
```
## TODOs
* Export functionality