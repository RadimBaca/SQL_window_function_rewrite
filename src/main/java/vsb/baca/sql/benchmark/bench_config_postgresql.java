package vsb.baca.sql.benchmark;

import vsb.baca.sql.model.Config;

import java.util.ArrayList;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class bench_config_postgresql extends bench_config {
    public String SET_PARALLEL_WORKERS;
    public String SET_PARALLEL_WORKERS_PER_GATHER;


    public bench_config_postgresql(String SQL_RANK_TEST_ROWNUMBER_CREATEINDEXES_SQL,
                                   String SQL_RANK_TEST_ROWNUMBER_DROPINDEXES_SQL,
                                   String CONNECTION_STRING,
                                   String username,
                                   String password,
                                   Config config,
                                   Logger logger,
                                   FileHandler fileHandler,
                                   ArrayList<String> queryFileNames,
                                   String tab_prefix,
                                   String set_parallel_workers,
                                   String set_parallel_workers_per_gather,
                                   Padding padding,
                                   Storage storage,
                                   Parallelism parallelism) {
        super(SQL_RANK_TEST_ROWNUMBER_CREATEINDEXES_SQL,
                SQL_RANK_TEST_ROWNUMBER_DROPINDEXES_SQL,
                CONNECTION_STRING,
                username,
                password,
                config,
                logger,
                fileHandler,
                queryFileNames,
                tab_prefix,
                storage,
                padding,
                parallelism);
        SET_PARALLEL_WORKERS = set_parallel_workers;
        SET_PARALLEL_WORKERS_PER_GATHER = set_parallel_workers_per_gather;
    }
}
