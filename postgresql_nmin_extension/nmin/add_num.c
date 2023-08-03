#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "catalog/pg_type.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PGDLLEXPORT Datum add_number(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum add_number_final(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(add_number);
PG_FUNCTION_INFO_V1(add_number_final);



Datum add_number_final(PG_FUNCTION_ARGS)
{
    ArrayType* input_arr = PG_GETARG_ARRAYTYPE_P(0);
    int32 insert_var = PG_GETARG_INT32(1);
    int input_len = ARR_DIMS(input_arr)[0];
    Datum* input_elems;
    bool* input_nulls;
    int j = input_len - 1;
    ArrayType* new_arr;

    deconstruct_array(input_arr, INT4OID, 4, true, 'i', &input_elems, &input_nulls, &input_len);

    int32 result = DatumGetInt32(input_elems[j]);
    if (result == 2147483647)
    {
        PG_RETURN_NULL();
    }

    PG_RETURN_INT32(result);
}



Datum add_number(PG_FUNCTION_ARGS)
{
    ArrayType* input_arr = PG_GETARG_ARRAYTYPE_P(0);
    int32 insert_var = PG_GETARG_INT32(1);
    int32 N = PG_GETARG_INT32(2);
    int input_len = ARR_DIMS(input_arr)[0];
    Datum* input_elems;
    bool* input_nulls;
    int j = input_len - 1;
    ArrayType* new_arr;
    unsigned short i = 0;

    //char str[20];
    //sprintf(str, "N: %d, input_len: %d", N, input_len);
    //elog(NOTICE, str);
    elog(NOTICE, "---------------");

    if (input_len < N)
    {
        // this is possible just during the first call of the aggregate
        // elog(NOTICE, "Array smaller than N, so we reset it");
        input_elems = (Datum*)palloc(N * sizeof(Datum));

        input_elems[i] = insert_var;
        for (i = 1; i < N; i++) {
            input_elems[i] = Int32GetDatum(2147483647);
        }

        new_arr = construct_array(input_elems, N, INT4OID, 4, true, 'i');

        PG_RETURN_ARRAYTYPE_P(new_arr);

    }
    else
    {
        deconstruct_array(input_arr, INT4OID, 4, true, 'i', &input_elems, &input_nulls, &input_len);

        if (DatumGetInt32(input_elems[j]) > insert_var || DatumGetInt32(input_elems[j]) == 2147483647) {
            while ((DatumGetInt32(input_elems[j - 1]) > insert_var || DatumGetInt32(input_elems[j - 1]) == 2147483647) && j > 0) {
                input_elems[j] = input_elems[j - 1];
                j--;
            }

            input_elems[j] = Int32GetDatum(insert_var);

            new_arr = construct_array(input_elems, input_len, INT4OID, 4, true, 'i');

            PG_RETURN_ARRAYTYPE_P(new_arr);
        }
    }

    PG_RETURN_ARRAYTYPE_P(input_arr);
}

