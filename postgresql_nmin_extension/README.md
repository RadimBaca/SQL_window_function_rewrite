# PostgreSQL C-function

The `postgre_nmin_extention` directory contains a PostgreSQL C-function that implements the necessary logic to create PostgreSql aggregate function.
First it is necessary to [compile](https://www.postgresql.org/docs/current/xfunc-c.html) the C-function. 
You need to have PostgreSQL installed on your computer. 
Copy the *.so/*.dll files and libraries to the PostgreSQL library directory. 
Then run the `nmin.sql` script to create the function in the database.

To test that the function works correctly you may run the `test_nmin.sql` script.

## NULL values and 2147483647

Unfortunately the NMIN function does not work correctly with NULL values. 
NULL values are automatically transformed into 0 in the NMIN function transition function.
We use the 2147483647 value as a NULL value replacement in the array.
Therefore, if you have 2147483647 value in the data then the NMIN function may not work correctly.