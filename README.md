# SQL Rewriting Tool

The main part of this repository is a Java SQL rewriting tool. There is also Python code for a result analysis and PostgreSQL NMIN C-function. 

The tool works in two modes:
1. Microbenchmark - performs a test suite on a prepared database
2. Rewrite - rewrites the input SQL query with the window function to a SQL query without it.

The tool uses ANTLR MSSql grammar to parse the SQL query, therefore, it may fail to rewrite SQL with syntax specific to other database systems.

## Build 

```shell
git clone https://github.com/RadimBaca/SQL_window_function_rewrite/
mvn clean install
mvn package
```

It creates the sqlrewriting-1.0-SNAPSHOT.jar file in the target directory.

## Microbenchmark

Microbenchmark mode is run with `-m` option.

We need to specify the microbenchmark type using the `-t` option. There are two basic microbenchmarks types:
- **Rank_alg** - compares the efficiency of different logical trees for the greatest per group problem.
- **Agg** - runs an aggregate window function microbenchmark.

There are also other microbenchmark type options that may be used together with `-t` option:
- **Rank** - runs a rank window function microbenchmark. Very similar to rank_alg but it uses the best logical tree. It is not used in the paper.
- **JoinNMin** - this is available only for PostgreSQL 
- **Unit_test** - runs few unit_test. Requires specific database setup (sql/unittest directory).
- **Probe** - runs just a single query.

### Run Microbenchmark

Prepare a database using appropriate SQL scripts in the SQL directory. For example, if you want to run the Agg microbenchmark for MSSql then you need to run the `sql/agg_test/init_mssql.sql` script in your database. It will create a set of tables that will be used during the microbenchmark.

Run the microbenchmark on the database with the following command:

```shell
java -jar target/sqlrewriting-1.0-SNAPSHOT.jar -m -d MSSql -h hostname.com;instanceName=sqldb;databaseName=sqlbench_window -u username -p password -t agg
```

## Run SQL Query Rewrite

Rewrite mode is run with `-r` option.

The necessary options are 
- `-d` - database system (MSSql, PostgreSql, Oracle)
- `-l` - type of logical tree (LateralAgg, LateralLimitTies, LateralDistinctLimitTies, JoinMin, BestFit)
- `-f` - file name with input SQL query

The output is a rewritten SQL query. Please remember that despite the fact that we can specify the DBMS type, the input SQL query must correspond to the MSSql syntax. Rewriter assumes that the ORDER BY attributes in the window function can not be NULL. This behavior can be changed in the wf_microbenchmark::rewriteSQL method.

An example of the rewrite:

```shell
java -jar target/sqlrewriting-1.0-SNAPSHOT.jar -r -d PostgreSQL -l BestFit -f sql/unittests/input_test.sql
```

# Build ANTLR4 classes

ANTLR4 classes are created automatically when the Maven project is run. If necessary you may create the ANTLR4 classes separately.
You need an ANTRL version 4 [to be installed on your computer](https://github.com/antlr/antlr4/blob/master/doc/getting-started.md). 
After you install ANTLR you need to create ANTLR Java classes from the grammar. On Windows you may run compile_grammar.bat in order to create the classes. It runs the following commands:

```shell
antlr4 grammar/Mssql_lexer.g4 -o src\main\java\vsb\baca\grammar 
antlr4 grammar/Mssql.g4 -visitor -o src\main\java\vsb\baca\grammar
```

