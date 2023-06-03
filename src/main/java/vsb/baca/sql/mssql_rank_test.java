package vsb.baca.sql;


import vsb.baca.sql.benchmark.bench_config;
import vsb.baca.sql.benchmark.bench_config_mssql;
import vsb.baca.sql.benchmark.benchmark_mssql;
import vsb.baca.sql.benchmark.run_setup;
import vsb.baca.sql.model.Config;

import java.util.ArrayList;
import java.util.logging.*;

public class mssql_rank_test {

    private static final String SQL_ROWNUMBER_EQUAL_1_PADDING_FILENAME = "sql/rank_test/rownumber_equal1_padding.sql";
    private static final String SQL_ROWNUMBER_EQUAL_N_PADDING_FILENAME = "sql/rank_test/rownumber_equalN_padding.sql";
    private static final String SQL_ROWNUMBER_LESS_N_PADDING_FILENAME = "sql/rank_test/rownumber_lessN_padding.sql";
    private static final String SQL_ROWNUMBER_EQUAL_1_FILENAME = "sql/rank_test/rownumber_equal1.sql";
    private static final String SQL_ROWNUMBER_EQUAL_N_FILENAME = "sql/rank_test/rownumber_equalN.sql";
    private static final String SQL_ROWNUMBER_LESS_N_FILENAME = "sql/rank_test/rownumber_lessN.sql";
    private static ArrayList<run_setup> run_setups = new ArrayList<run_setup>();

    private static final String DROPINDEXES_FILENAME = "sql/rank_test/rownumber_dropindexes.sql";
    private static final String CREATEINDEXES_FILENAME = "sql/rank_test/rownumber_createindexes.sql";
    private static final String CONNECTION_STRING = "jdbc:sqlserver://bayer.cs.vsb.cz;instanceName=sqldb;databaseName=sqlbench_window;;user=sqlbench;password=n3cUmubsbo";
    private static Config config = new Config(Config.dbms.MSSQL, false, true);
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
            fileHandler = new FileHandler("log.txt", 1024 * 1024, 1, true);
            logger.addHandler(fileHandler);
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);
        } catch (Exception e) {
            e.printStackTrace();
        }

        ArrayList<String> queryFileNamesPadding = new ArrayList<String>();
        queryFileNamesPadding.add(SQL_ROWNUMBER_EQUAL_1_PADDING_FILENAME);
        queryFileNamesPadding.add(SQL_ROWNUMBER_EQUAL_N_PADDING_FILENAME);
        queryFileNamesPadding.add(SQL_ROWNUMBER_LESS_N_PADDING_FILENAME);
        ArrayList<String> queryFileNamesNoPadding = new ArrayList<String>();
        queryFileNamesNoPadding.add(SQL_ROWNUMBER_EQUAL_1_FILENAME);
        queryFileNamesNoPadding.add(SQL_ROWNUMBER_EQUAL_N_FILENAME);
        queryFileNamesNoPadding.add(SQL_ROWNUMBER_LESS_N_FILENAME);

//        run_setups.add(new run_setup("R_row_", queryFileNamesNoPadding, "\nOPTION(MAXDOP 1)", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.OFF));
//        run_setups.add(new run_setup("R_column_", queryFileNamesNoPadding, "\nOPTION(MAXDOP 1)", bench_config.Padding.OFF, bench_config.Storage.COLUMN, bench_config.Parallelism.OFF));
//        run_setups.add(new run_setup("P_row_", queryFileNamesPadding, "\nOPTION(MAXDOP 1)", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.OFF));
//        run_setups.add(new run_setup("P_column_", queryFileNamesPadding, "\nOPTION(MAXDOP 1)", bench_config.Padding.ON, bench_config.Storage.COLUMN, bench_config.Parallelism.OFF));
//        run_setups.add(new run_setup("R_row_", queryFileNamesNoPadding, "\nOPTION(MAXDOP 8)", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.ON));
//        run_setups.add(new run_setup("R_column_", queryFileNamesNoPadding, "\nOPTION(MAXDOP 8)", bench_config.Padding.OFF, bench_config.Storage.COLUMN, bench_config.Parallelism.ON));
//        run_setups.add(new run_setup("P_row_", queryFileNamesPadding, "\nOPTION(MAXDOP 8)", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.ON));
//        run_setups.add(new run_setup("P_column_", queryFileNamesPadding, "\nOPTION(MAXDOP 8)", bench_config.Padding.ON, bench_config.Storage.COLUMN, bench_config.Parallelism.ON));

        for (run_setup run_setup : run_setups) {
            bench_config_mssql bench_config = new bench_config_mssql(CREATEINDEXES_FILENAME, DROPINDEXES_FILENAME,
                    CONNECTION_STRING, "", "", config, logger, fileHandler,
                    run_setup.queryFileNames, run_setup.tab_prefix, run_setup.option_maxdop, run_setup.padding, run_setup.storage, run_setup.parallelism);

            benchmark_mssql rank_benchmark = new benchmark_mssql(bench_config);
            rank_benchmark.run();
        }

        fileHandler.close();
    }
}

