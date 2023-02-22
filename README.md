# Rewrite an SQL with window function

Application rewrite an SQL command with an window function to a SQL without window function. Rewritter currently support MS Sql.

# Build from source code

You need a ANTRL version 4 [to be installed on your computer](https://github.com/antlr/antlr4/blob/master/doc/getting-started.md). 
After you install ANTLR then you need to create ANTLR Java classes from the grammar. On windows you may run compile_grammar.bat in order to create the classes. It runs the following commands:

```shell
antlr4 grammar/Mssql_lexer.g4 -o src\main\java\vsb\baca\grammar 
antlr4 grammar/Mssql.g4 -visitor -o src\main\java\vsb\baca\grammar
```

