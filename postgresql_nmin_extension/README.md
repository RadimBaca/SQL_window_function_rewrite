# PostgreSQL C-function

The `postgre_nmin_extention` directory contains a PostgreSQL C-function that implements the necessary logic to create PostgreSql aggregate function.
First it is necessary to [compile](https://www.postgresql.org/docs/current/xfunc-c.html) the C-function. 
You need to have PostgreSQL installed on your computer. 
Copy the *.so/*.dll files and libraries to the PostgreSQL library directory. 
Then run the `nmin.sql` script to create the function in the database.

To test that the function works correctly you may run the `test_nmin.sql` script.

