package vsb.baca.sql;


import org.antlr.v4.runtime.misc.Pair;
import vsb.baca.sql.benchmark.*;
import vsb.baca.sql.model.Config;

import java.util.ArrayList;
import java.util.logging.*;

public class postgre_agg_pb_test {

    private static final String SUM_PB_TEMP = "sql/agg_test/count_pb_temp.sql";
    private static final String SUM_PB_TEMP_PADDING = "sql/agg_test/count_pb_temp_padding.sql";

    private static ArrayList<run_setup_postgresql> run_setups = new ArrayList<run_setup_postgresql>();

    private static final String DROPINDEXES_FILENAME = "sql/agg_test/agg_pb_dropindexes_postgre.sql";
    private static final String CREATEINDEXES_FILENAME = "sql/agg_test/agg_pb_createindexes.sql";
    private static final String CONNECTION_STRING = "jdbc:postgresql://bayer.cs.vsb.cz:5433/sqlbench";
    private static final String USERNAME = "sqlbench";
    private static final String PASSWORD = "n3cUmubsbo";
    private static final String SET_PARALLEL_WORKERS_0 = "SET max_parallel_workers = 0";
    private static final String SET_PARALLEL_WORKERS_PER_GATHER_0 = "SET max_parallel_workers_per_gather = 0";
    private static final String SET_PARALLEL_WORKERS_N = "SET max_parallel_workers = 8"; // we allow to use any number of parallel threads
    private static final String SET_PARALLEL_WORKERS_PER_GATHER_N = "SET max_parallel_workers_per_gather = 8"; // we allow to use any number of parallel threads per gather

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

        ////////////////////////////////////////////
        ArrayList<Integer> selectivity = new ArrayList<Integer>();
        int init_sel = 1;
        for (int i = 0; i < 6; i++) {
            selectivity.add(init_sel);
            init_sel *= 2;
        }

        ArrayList<Pair<String, String>> queryFileNamesNoPadding = benchmark_mssql.generateQueriesWithSelectivity(selectivity, SUM_PB_TEMP);
        ArrayList<Pair<String, String>> queryFileNamesPadding = benchmark_mssql.generateQueriesWithSelectivity(selectivity, SUM_PB_TEMP_PADDING);

        run_setups.add(new run_setup_postgresql("R_row_", queryFileNamesNoPadding, SET_PARALLEL_WORKERS_0, SET_PARALLEL_WORKERS_PER_GATHER_0, bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.OFF));
        run_setups.add(new run_setup_postgresql("P_row_", queryFileNamesPadding, SET_PARALLEL_WORKERS_0, SET_PARALLEL_WORKERS_PER_GATHER_0, bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.OFF));
        run_setups.add(new run_setup_postgresql("R_row_", queryFileNamesNoPadding,  SET_PARALLEL_WORKERS_N, SET_PARALLEL_WORKERS_PER_GATHER_N, bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.ON));
        run_setups.add(new run_setup_postgresql("P_row_", queryFileNamesPadding,  SET_PARALLEL_WORKERS_N, SET_PARALLEL_WORKERS_PER_GATHER_N, bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.ON));

//        run_setups.add(new run_setup("R_row_", queriesNoPadding, "\nOPTION(MAXDOP 1)", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.OFF));
//        run_setups.add(new run_setup("R_column_", queriesNoPadding, "\nOPTION(MAXDOP 1)", bench_config.Padding.OFF, bench_config.Storage.COLUMN, bench_config.Parallelism.OFF));
//        run_setups.add(new run_setup("P_row_", queriesPadding, "\nOPTION(MAXDOP 1)", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.OFF));
//        run_setups.add(new run_setup("P_column_", queriesPadding, "\nOPTION(MAXDOP 1)", bench_config.Padding.ON, bench_config.Storage.COLUMN, bench_config.Parallelism.OFF));
//        run_setups.add(new run_setup("R_row_", queriesNoPadding, "\nOPTION(MAXDOP 8)", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.ON));
//        run_setups.add(new run_setup("R_column_", queriesNoPadding, "\nOPTION(MAXDOP 8)", bench_config.Padding.OFF, bench_config.Storage.COLUMN, bench_config.Parallelism.ON));
//        run_setups.add(new run_setup("P_row_", queriesPadding, "\nOPTION(MAXDOP 8)", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.ON));
//        run_setups.add(new run_setup("P_column_", queriesPadding, "\nOPTION(MAXDOP 8)", bench_config.Padding.ON, bench_config.Storage.COLUMN, bench_config.Parallelism.ON));

        for (run_setup_postgresql run_setup : run_setups) {
            bench_config_postgresql bench_config = new bench_config_postgresql(CREATEINDEXES_FILENAME, DROPINDEXES_FILENAME,
                    CONNECTION_STRING, USERNAME, PASSWORD, config, logger, fileHandler,
                    run_setup.queryFileNames, run_setup.tab_prefix, run_setup.SET_PARALLEL_WORKERS, run_setup.SET_PARALLEL_WORKERS_PER_GATHER,
                    run_setup.padding, run_setup.storage, run_setup.parallelism);

            benchmark_postgresql rank_benchmark = new benchmark_postgresql(bench_config);
            rank_benchmark.run();
        }

        fileHandler.close();
    }


}

