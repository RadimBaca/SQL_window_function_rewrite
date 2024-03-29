package vsb.baca.sql;


import org.antlr.v4.runtime.misc.Pair;
import vsb.baca.sql.benchmark.bench_config;
import vsb.baca.sql.benchmark.benchmark_mssql;
import vsb.baca.sql.benchmark.run_setup;
import vsb.baca.sql.model.Config;

import java.util.ArrayList;
import java.util.logging.*;

public class mssql_rank_algorithm_test {

    private static final String SQL_ROWNUMBER_EQUAL_1_PADDING_FILENAME = "sql/rank_test/rownumber_equal1_padding.sql";
    private static final String SQL_ROWNUMBER_EQUAL_1_FILENAME = "sql/rank_test/rownumber_equal1.sql";
    private static ArrayList<run_setup> run_setups = new ArrayList<run_setup>();

    private static final String DROPINDEXES_FILENAME = "sql/rank_test/rownumber_dropindexes_mssql.sql";
    private static final String CREATEINDEXES_FILENAME = "sql/rank_test/rownumber_createindexes.sql";


    private static String connection_string;
    private static String username;
    private static String password;
    private static String storage;

    public static void run(String connection_string, String username, String password, String storage) throws Exception {

        mssql_rank_algorithm_test.connection_string = connection_string;
        mssql_rank_algorithm_test.username = username;
        mssql_rank_algorithm_test.password = password;
        mssql_rank_algorithm_test.storage = storage;

        ArrayList<Pair<String,String>> queryFileNamesPadding = new ArrayList<Pair<String,String>>();
        queryFileNamesPadding.add(benchmark_mssql.readQueryFromFile(SQL_ROWNUMBER_EQUAL_1_PADDING_FILENAME));
        ArrayList<Pair<String,String>> queryFileNamesNoPadding = new ArrayList<Pair<String,String>>();
        queryFileNamesNoPadding.add(benchmark_mssql.readQueryFromFile(SQL_ROWNUMBER_EQUAL_1_FILENAME));

        ArrayList<Config> configs = new ArrayList<Config>();
        configs.add(new Config(Config.dbms.MSSQL, false, Config.rank_algorithm.LateralAgg));
        configs.add(new Config(Config.dbms.MSSQL, false, Config.rank_algorithm.JoinMin));
        configs.add(new Config(Config.dbms.MSSQL, false, Config.rank_algorithm.LateralDistinctLimit));
        configs.add(new Config(Config.dbms.MSSQL, false, Config.rank_algorithm.LateralLimit));

        for (Config config : configs) {
//            run_setups.add(new run_setup("R_row_", queryFileNamesNoPadding, "", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.ON, config, mssql_rank_algorithm_test.connection_string, mssql_rank_algorithm_test.username, mssql_rank_algorithm_test.password));

            if (mssql_rank_algorithm_test.storage.contains("row")) {
                run_setups.add(new run_setup("R_row_", queryFileNamesNoPadding, "", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.OFF, config, mssql_rank_algorithm_test.connection_string, mssql_rank_algorithm_test.username, mssql_rank_algorithm_test.password));
                run_setups.add(new run_setup("P_row_", queryFileNamesPadding, "", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.OFF, config, mssql_rank_algorithm_test.connection_string, mssql_rank_algorithm_test.username, mssql_rank_algorithm_test.password));
                run_setups.add(new run_setup("R_row_", queryFileNamesNoPadding, "", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.ON, config, mssql_rank_algorithm_test.connection_string, mssql_rank_algorithm_test.username, mssql_rank_algorithm_test.password));
                run_setups.add(new run_setup("P_row_", queryFileNamesPadding, "", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.ON, config, mssql_rank_algorithm_test.connection_string, mssql_rank_algorithm_test.username, mssql_rank_algorithm_test.password));
            }
            if (mssql_rank_algorithm_test.storage.contains("column")) {
                run_setups.add(new run_setup("R_column_", queryFileNamesNoPadding, "", bench_config.Padding.OFF, bench_config.Storage.COLUMN, bench_config.Parallelism.OFF, config, mssql_rank_algorithm_test.connection_string, mssql_rank_algorithm_test.username, mssql_rank_algorithm_test.password));
                run_setups.add(new run_setup("P_column_", queryFileNamesPadding, "", bench_config.Padding.ON, bench_config.Storage.COLUMN, bench_config.Parallelism.OFF, config, mssql_rank_algorithm_test.connection_string, mssql_rank_algorithm_test.username, mssql_rank_algorithm_test.password));
                run_setups.add(new run_setup("R_column_", queryFileNamesNoPadding, "", bench_config.Padding.OFF, bench_config.Storage.COLUMN, bench_config.Parallelism.ON, config, mssql_rank_algorithm_test.connection_string, mssql_rank_algorithm_test.username, mssql_rank_algorithm_test.password));
                run_setups.add(new run_setup("P_column_", queryFileNamesPadding, "", bench_config.Padding.ON, bench_config.Storage.COLUMN, bench_config.Parallelism.ON, config, mssql_rank_algorithm_test.connection_string, mssql_rank_algorithm_test.username, mssql_rank_algorithm_test.password));
            }
        }

        mssql_runner.prepare_run(run_setups, DROPINDEXES_FILENAME, CREATEINDEXES_FILENAME);






//        // Disable console logging
//        Logger rootLogger = Logger.getLogger("");
//        Handler[] handlers = rootLogger.getHandlers();
//        for (Handler handler : handlers) {
//            if (handler instanceof ConsoleHandler) {
//                rootLogger.removeHandler(handler);
//            }
//        }
//
//        try {
//            // Set up the file path and limit the log file size
//            fileHandler = new FileHandler("log_mssql.txt", 1024 * 1024, 1, true);
//            logger.addHandler(fileHandler);
//            SimpleFormatter formatter = new SimpleFormatter();
//            fileHandler.setFormatter(formatter);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        ArrayList<Pair<String,String>> queryFileNamesPadding = new ArrayList<Pair<String,String>>();
//        queryFileNamesPadding.add(benchmark_mssql.readQueryFromFile(SQL_ROWNUMBER_EQUAL_1_PADDING_FILENAME));
//        ArrayList<Pair<String,String>> queryFileNamesNoPadding = new ArrayList<Pair<String,String>>();
//        queryFileNamesNoPadding.add(benchmark_mssql.readQueryFromFile(SQL_ROWNUMBER_EQUAL_1_FILENAME));
//
//        ArrayList<Config> configs = new ArrayList<Config>();
//        configs.add(new Config(Config.dbms.MSSQL, false, Config.rank_algorithm.LateralAgg));
//        configs.add(new Config(Config.dbms.MSSQL, false, Config.rank_algorithm.LateralLimit));
//        configs.add(new Config(Config.dbms.MSSQL, false, Config.rank_algorithm.LateralDistinctLimit));
//        configs.add(new Config(Config.dbms.MSSQL, false, Config.rank_algorithm.JoinMin));
//
//        for (Config config : configs) {
//            run_setups.add(new run_setup("R_row_", queryFileNamesNoPadding, "\nOPTION(MAXDOP 1)",
//                    bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.OFF, config));
//            run_setups.add(new run_setup("P_row_", queryFileNamesPadding, "\nOPTION(MAXDOP 1)",
//                    bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.OFF, config));
//            run_setups.add(new run_setup("R_row_", queryFileNamesNoPadding, "\nOPTION(MAXDOP 8)",
//                    bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.ON, config));
//            run_setups.add(new run_setup("P_row_", queryFileNamesPadding, "\nOPTION(MAXDOP 8)",
//                    bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.ON, config));
////            run_setups.add(new run_setup("R_column_", queryFileNamesNoPadding, "\nOPTION(MAXDOP 1)",
////                    bench_config.Padding.OFF, bench_config.Storage.COLUMN, bench_config.Parallelism.OFF, config));
////            run_setups.add(new run_setup("P_column_", queryFileNamesPadding, "\nOPTION(MAXDOP 1)",
////                    bench_config.Padding.ON, bench_config.Storage.COLUMN, bench_config.Parallelism.OFF, config));
////            run_setups.add(new run_setup("R_column_", queryFileNamesNoPadding, "\nOPTION(MAXDOP 8)",
////                    bench_config.Padding.OFF, bench_config.Storage.COLUMN, bench_config.Parallelism.ON, config));
////            run_setups.add(new run_setup("P_column_", queryFileNamesPadding, "\nOPTION(MAXDOP 8)",
////                    bench_config.Padding.ON, bench_config.Storage.COLUMN, bench_config.Parallelism.ON, config));
//        }
//
//        for (run_setup run_setup : run_setups) {
//            bench_config_mssql bench_config = new bench_config_mssql(CREATEINDEXES_FILENAME, DROPINDEXES_FILENAME,
//                    CONNECTION_STRING, "", "", run_setup.config, logger, fileHandler,
//                    run_setup.queryFileNames, run_setup.tab_prefix, run_setup.option_maxdop, run_setup.padding, run_setup.storage, run_setup.parallelism);
//
//            benchmark_mssql rank_benchmark = new benchmark_mssql(bench_config);
//            rank_benchmark.run();
//        }
//
//        fileHandler.close();
    }
}

