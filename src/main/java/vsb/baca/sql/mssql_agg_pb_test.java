package vsb.baca.sql;


import org.antlr.v4.runtime.misc.Pair;
import vsb.baca.sql.benchmark.bench_config;
import vsb.baca.sql.benchmark.bench_config_mssql;
import vsb.baca.sql.benchmark.benchmark_mssql;
import vsb.baca.sql.benchmark.run_setup;
import vsb.baca.sql.model.Config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.*;

import static vsb.baca.sql.benchmark.benchmark.generateQueriesWithSelectivity;

public class mssql_agg_pb_test {

    private static final String SUM_PB_TEMP = "sql/agg_test/count_pb_temp.sql";
    private static final String SUM_PB_TEMP_PADDING = "sql/agg_test/count_pb_temp_padding.sql";

    private static ArrayList<run_setup> run_setups = new ArrayList<run_setup>();

    private static final String DROPINDEXES_FILENAME = "sql/agg_test/agg_pb_dropindexes_mssql.sql";
    private static final String CREATEINDEXES_FILENAME = "sql/agg_test/agg_pb_createindexes.sql";
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

        ////////////////////////////////////////////
        ArrayList<Integer> selectivity = new ArrayList<Integer>();
        int init_sel = 1;
        for (int i = 0; i < 6; i++) {
            selectivity.add(init_sel);
            init_sel *= 2;
        }

        ArrayList<Pair<String, String>> queriesNoPadding = benchmark_mssql.generateQueriesWithSelectivity(selectivity, SUM_PB_TEMP);
        ArrayList<Pair<String, String>> queriesPadding = benchmark_mssql.generateQueriesWithSelectivity(selectivity, SUM_PB_TEMP_PADDING);

        run_setups.add(new run_setup("R_row_", queriesNoPadding, "\nOPTION(MAXDOP 1)", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.OFF));
        run_setups.add(new run_setup("R_column_", queriesNoPadding, "\nOPTION(MAXDOP 1)", bench_config.Padding.OFF, bench_config.Storage.COLUMN, bench_config.Parallelism.OFF));
        run_setups.add(new run_setup("P_row_", queriesPadding, "\nOPTION(MAXDOP 1)", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.OFF));
        run_setups.add(new run_setup("P_column_", queriesPadding, "\nOPTION(MAXDOP 1)", bench_config.Padding.ON, bench_config.Storage.COLUMN, bench_config.Parallelism.OFF));
        run_setups.add(new run_setup("R_row_", queriesNoPadding, "\nOPTION(MAXDOP 8)", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.ON));
        run_setups.add(new run_setup("R_column_", queriesNoPadding, "\nOPTION(MAXDOP 8)", bench_config.Padding.OFF, bench_config.Storage.COLUMN, bench_config.Parallelism.ON));
        run_setups.add(new run_setup("P_row_", queriesPadding, "\nOPTION(MAXDOP 8)", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.ON));
        run_setups.add(new run_setup("P_column_", queriesPadding, "\nOPTION(MAXDOP 8)", bench_config.Padding.ON, bench_config.Storage.COLUMN, bench_config.Parallelism.ON));

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

