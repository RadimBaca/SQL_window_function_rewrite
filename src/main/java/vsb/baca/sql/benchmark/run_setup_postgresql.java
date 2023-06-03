package vsb.baca.sql.benchmark;


import org.antlr.v4.runtime.misc.Pair;

import java.util.ArrayList;

public class run_setup_postgresql extends run_setup
{
    public String SET_PARALLEL_WORKERS;
    public String SET_PARALLEL_WORKERS_PER_GATHER;

    public run_setup_postgresql(String tab_prefix,
                                ArrayList<Pair<String, String>> queryFileNames,
                                String set_parallel_workers,
                                String set_parallel_workers_per_gather,
                                bench_config.Padding padding,
                                bench_config.Storage storage,
                                bench_config.Parallelism parallelism) {
        super(tab_prefix, queryFileNames, "", padding, storage, parallelism);
        SET_PARALLEL_WORKERS = set_parallel_workers;
        SET_PARALLEL_WORKERS_PER_GATHER = set_parallel_workers_per_gather;
    }
}
