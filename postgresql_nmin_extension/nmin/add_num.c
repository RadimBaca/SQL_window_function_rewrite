#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "catalog/pg_type.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PGDLLEXPORT Datum add_number(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(add_number);


Datum add_number(PG_FUNCTION_ARGS) 
{
    ArrayType *input_arr = PG_GETARG_ARRAYTYPE_P(0);
    int32 insert_var = PG_GETARG_INT32(1);
    int input_len = ARR_DIMS(input_arr)[0];
    Datum *input_elems;
    bool *input_nulls;
    int j = input_len - 1;
    ArrayType *new_arr;

    deconstruct_array(input_arr, INT4OID, 4, true, 'i', &input_elems, &input_nulls, &input_len);

    if (DatumGetInt32(input_elems[j]) > insert_var) {
        while (DatumGetInt32(input_elems[j - 1]) > insert_var && j > 0) {
            input_elems[j] = input_elems[j - 1];
            j--;
        }
        input_elems[j] = Int32GetDatum(insert_var);

        new_arr = construct_array(input_elems, input_len, INT4OID, 4, true, 'i');

        PG_RETURN_ARRAYTYPE_P(new_arr);
    }

    PG_RETURN_ARRAYTYPE_P(input_arr);
} 
