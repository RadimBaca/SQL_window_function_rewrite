package vsb.baca.sql;


import vsb.baca.sql.benchmark.bench_config;
import vsb.baca.sql.benchmark.bench_config_postgresql;
import vsb.baca.sql.benchmark.benchmark_postgresql;
import vsb.baca.sql.benchmark.run_setup_postgresql;
import vsb.baca.sql.model.Config;

import java.util.ArrayList;
import java.util.logging.*;

public class postgre_agg_test {

    private static final String SUM_PBOB_LESS01 = "sql/agg_test/sum_pb_or_sel01.sql";
    private static final String SUM_PBOB_LESS03 = "sql/agg_test/sum_pb_or_sel03.sql";
    private static final String SUM_PBOB_LESS10 = "sql/agg_test/sum_pb_or_sel10.sql";
    private static final String SUM_PBOB_LESS30 = "sql/agg_test/sum_pb_or_sel30.sql";
    private static final String SUM_PBOB_LESS100 = "sql/agg_test/sum_pb_or_sel100.sql";
    private static final String SUM_PBOB_LESS300 = "sql/agg_test/sum_pb_or_sel300.sql";


    private static final String SUM_PBOB_LESS01_PADDING = "sql/agg_test/sum_pb_or_sel01_padding.sql";
    private static final String SUM_PBOB_LESS03_PADDING = "sql/agg_test/sum_pb_or_sel03_padding.sql";
    private static final String SUM_PBOB_LESS10_PADDING = "sql/agg_test/sum_pb_or_sel10_padding.sql";
    private static final String SUM_PBOB_LESS30_PADDING = "sql/agg_test/sum_pb_or_sel30_padding.sql";
    private static final String SUM_PBOB_LESS100_PADDING = "sql/agg_test/sum_pb_or_sel100_padding.sql";
    private static final String SUM_PBOB_LESS300_PADDING = "sql/agg_test/sum_pb_or_sel300_padding.sql";

    private static ArrayList<run_setup_postgresql> run_setups = new ArrayList<run_setup_postgresql>();

    private static final String CONNECTION_STRING = "jdbc:postgresql://bayer.cs.vsb.cz:5433/sqlbench";
    private static final String USERNAME = "sqlbench";
    private static final String PASSWORD = "n3cUmubsbo";
    private static final String DROPINDEXES_FILENAME = "sql/rank_test/rownumber_dropindexes_postgre.sql";
    private static final String CREATEINDEXES_FILENAME = "sql/rank_test/rownumber_createindexes.sql";
    private static final String SET_PARALLEL_WORKERS_0 = "SET max_parallel_workers = 0";
    private static final String SET_PARALLEL_WORKERS_PER_GATHER_0 = "SET max_parallel_workers_per_gather = 0";
    private static final String SET_PARALLEL_WORKERS_N = "SET max_parallel_workers = 8";
    private static final String SET_PARALLEL_WORKERS_PER_GATHER_N = "SET max_parallel_workers_per_gather = 0";
    private static Config config = new Config(Config.dbms.POSTGRESQL, false, true);
    private static Logger logger = Logger.getLogger("MyLogger");
    private static FileHandler fileHandler;

    public static void main(String[] args) throws Exception {

        // Disable console logging
        Logger rootLogger = Logger.getLogger("");
        Handler[] handlers = rootLogger.getHandlers();
        for (Handler handler : handlers) {
            if (handler instanceof ConsoleHandler) {
                rootLogger.removeHandler(handler);
            }
        }

        try {
            // Set up the file path and limit the log file size
            fileHandler = new FileHandler("log_postgresql.txt", 1024 * 1024, 1, true);
            logger.addHandler(fileHandler);
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);
        } catch (Exception e) {
            e.printStackTrace();
        }

        ArrayList<String> queryFileNamesPadding = new ArrayList<String>();
        queryFileNamesPadding.add(SUM_PBOB_LESS01_PADDING);
        queryFileNamesPadding.add(SUM_PBOB_LESS03_PADDING);
        queryFileNamesPadding.add(SUM_PBOB_LESS10_PADDING);
        queryFileNamesPadding.add(SUM_PBOB_LESS30_PADDING);
        queryFileNamesPadding.add(SUM_PBOB_LESS100_PADDING);
        queryFileNamesPadding.add(SUM_PBOB_LESS300_PADDING);

        ArrayList<String> queryFileNamesNoPadding = new ArrayList<String>();
        queryFileNamesNoPadding.add(SUM_PBOB_LESS01);
        queryFileNamesNoPadding.add(SUM_PBOB_LESS03);
        queryFileNamesNoPadding.add(SUM_PBOB_LESS10);
        queryFileNamesNoPadding.add(SUM_PBOB_LESS30);
        queryFileNamesNoPadding.add(SUM_PBOB_LESS100);
        queryFileNamesNoPadding.add(SUM_PBOB_LESS300);

        run_setups.add(new run_setup_postgresql("R_row_", queryFileNamesNoPadding, SET_PARALLEL_WORKERS_0, SET_PARALLEL_WORKERS_PER_GATHER_0, bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.OFF));
        run_setups.add(new run_setup_postgresql("P_row_", queryFileNamesPadding, SET_PARALLEL_WORKERS_0, SET_PARALLEL_WORKERS_PER_GATHER_0, bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.OFF));
        run_setups.add(new run_setup_postgresql("R_row_", queryFileNamesNoPadding,  SET_PARALLEL_WORKERS_N, SET_PARALLEL_WORKERS_PER_GATHER_N, bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.ON));
        run_setups.add(new run_setup_postgresql("P_row_", queryFileNamesPadding,  SET_PARALLEL_WORKERS_N, SET_PARALLEL_WORKERS_PER_GATHER_N, bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.ON));

        for (run_setup_postgresql run_setup : run_setups) {
            bench_config_postgresql bench_config = new bench_config_postgresql(CREATEINDEXES_FILENAME, DROPINDEXES_FILENAME,
                    CONNECTION_STRING, USERNAME, PASSWORD, config, logger, fileHandler,
                    run_setup.queryFileNames, run_setup.tab_prefix, run_setup.SET_PARALLEL_WORKERS, run_setup.SET_PARALLEL_WORKERS_PER_GATHER, run_setup.padding, run_setup.storage, run_setup.parallelism);

            benchmark_postgresql rank_benchmark = new benchmark_postgresql(bench_config);
            rank_benchmark.run();
        }

        fileHandler.close();
    }

}

