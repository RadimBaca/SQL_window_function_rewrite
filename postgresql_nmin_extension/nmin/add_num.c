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
    int position = 0;
    int i;
    int input_len = ARR_DIMS(input_arr)[0];
    Datum *input_elems;
    bool *input_nulls;
    int new_len;
    int j = 0;
    int k = 0;
    int *new_elems;
    ArrayType *new_arr;

    deconstruct_array(input_arr, INT4OID, 4, true, 'i', &input_elems, &input_nulls, &input_len);

    for (i = 0; i < input_len; i++) {
        if (DatumGetInt32(input_elems[i]) > insert_var) {
            break;
        }
        position++;
    }

    if (position < input_len) {
        new_len = input_len + 1;
        new_elems = palloc(sizeof(int) * new_len);

        for (j = 0; j < position; j++) {
            new_elems[k++] = DatumGetInt32(input_elems[j]);
        }

        new_elems[k++] = insert_var;

        for (j = position; j < input_len; j++) {
            new_elems[k++] = DatumGetInt32(input_elems[j]);
        }

        new_arr = construct_array((Datum *) new_elems, new_len, INT4OID, 4, true, 'i');

        PG_RETURN_ARRAYTYPE_P(new_arr);
    }

    PG_RETURN_ARRAYTYPE_P(input_arr);
} 
