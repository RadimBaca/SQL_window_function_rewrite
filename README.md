# SQL Rewriting Tool

The main part of this repository is a Java SQL rewriting tool. There are also python code for result analysis and PostgreSQL C-function that are described at the end of this readme. 

The tool works in two modes:
1. Microbenchmark - performs a test suite on a prepared database
2. Rewrite - rewrites the input SQL query with the window function to a SQL query without it.

The tool uses ANTLR MSSql gramar to parse the SQL query, therefore, it may fail to rewrite SQL with syntax specific to other database systems.

## Microbenchmark

Microbenchmark mode is run with `-m` option.

There are two microbenchmarks:
- **Rank_alg** - compares efficiency of different logical trees for greatest per group problem.
- **Agg** - runs an aggregate window function microbenchmark.

There are also other test type options that may be used to with microbenchmark `-m` option:
- **Rank** - runs a rank window function microbenchmark. Very similar to rank_alg but it uses the best logical tree. It is not used in the paper.
- **JoinNMin** - this is available only for PostgreSql 
- **Unit_test** - runs few unit_test. Requires specific database setup (sql/unittest directory).
- **Probe** - runs just a single query.


## Build 

```shell
git clone https://github.com/RadimBaca/SQL_window_function_rewrite/
mvn clean install
mvn package
```

It creates the sql-1.0-SNAPSHOT.jar file in the target directory.

## Run 

Prepare a database using appropriate SQL scripts in the sql directory. For example if you want to run the Agg microbenchmark for MSSql then you need to run the `sql/agg_test/init_mssql.sql` script in your database. It will create a set of tables that will be used during the microbenchmark.

Run the tool with the following command:

```shell
java -jar target/sql-1.0-SNAPSHOT.jar -m -d MSSql -h hostname.com;instanceName=sqldb;databaseName=sqlbench_window -u username -p password -t agg
```

# Result analysis (python)

Files produced by the microbenchmark can be analyzed using python scripts in the `result_analysis` directory. 
The directory currently contains result text files for the microbenchmarks used in the paper. 
For example to get result graphs for the aggrewgate window function microbenchmark run the following command:

```shell
python agg_analysis.py
```

# PostgreSQL C-function

The `postgre_nmin_extention` directory contains a PostgreSQL C-function that implements the necessary logic to create PostgreSql aggregate function.
First it is necessary to [compile](https://www.postgresql.org/docs/current/xfunc-c.html) the C-function. 
You need to have PostgreSQL installed on your computer. 
Copy the *.so/*.dll files and libraries to the PostgreSQL library directory. 
Then run the `nmin.sql` script to create the function in the database.

To test that the function works correctly you may run the `test_nmin.sql` script.

# Build ANTLR4 classes

If necessary you may create the ANTLR4 classes separately.
You need a ANTRL version 4 [to be installed on your computer](https://github.com/antlr/antlr4/blob/master/doc/getting-started.md). 
After you install ANTLR then you need to create ANTLR Java classes from the grammar. On windows you may run compile_grammar.bat in order to create the classes. It runs the following commands:

```shell
antlr4 grammar/Mssql_lexer.g4 -o src\main\java\vsb\baca\grammar 
antlr4 grammar/Mssql.g4 -visitor -o src\main\java\vsb\baca\grammar
```

