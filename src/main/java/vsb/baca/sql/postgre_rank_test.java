package vsb.baca.sql;


import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.tree.ParseTree;
import vsb.baca.grammar.Mssql;
import vsb.baca.grammar.Mssql_lexer;
import vsb.baca.grammar.rewriter.Mssql_rewriter_visitor;
import vsb.baca.sql.benchmark.*;
import vsb.baca.sql.model.Config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.logging.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.pow;

public class postgre_rank_test {


    private static final String SQL_ROWNUMBER_EQUAL_1_PADDING_FILENAME = "sql/rank_test/rownumber_equal1_padding.sql";
    private static final String SQL_ROWNUMBER_EQUAL_N_PADDING_FILENAME = "sql/rank_test/rownumber_equalN_padding.sql";
    private static final String SQL_ROWNUMBER_LESS_N_PADDING_FILENAME = "sql/rank_test/rownumber_lessN_padding.sql";
    private static final String SQL_ROWNUMBER_EQUAL_1_FILENAME = "sql/rank_test/rownumber_equal1.sql";
    private static final String SQL_ROWNUMBER_EQUAL_N_FILENAME = "sql/rank_test/rownumber_equalN.sql";
    private static final String SQL_ROWNUMBER_LESS_N_FILENAME = "sql/rank_test/rownumber_lessN.sql";
    private static ArrayList<run_setup_postgresql> run_setups = new ArrayList<run_setup_postgresql>();

    private static final String CONNECTION_STRING = "jdbc:postgresql://bayer.cs.vsb.cz:5433/sqlbench";
    private static final String USERNAME = "sqlbench";
    private static final String PASSWORD = "n3cUmubsbo";
    private static final String DROPINDEXES_FILENAME = "sql/rank_test/rownumber_dropindexes_postgre.sql";
    private static final String CREATEINDEXES_FILENAME = "sql/rank_test/rownumber_createindexes.sql";
    private static final String SET_PARALLEL_WORKERS_0 = "SET max_parallel_workers = 0";
    private static final String SET_PARALLEL_WORKERS_PER_GATHER_0 = "SET max_parallel_workers_per_gather = 0";
    private static final String SET_PARALLEL_WORKERS_N = "SET max_parallel_workers = 8";
    private static final String SET_PARALLEL_WORKERS_PER_GATHER_N = "SET max_parallel_workers_per_gather = 8";
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
        queryFileNamesPadding.add(SQL_ROWNUMBER_EQUAL_1_PADDING_FILENAME);
        queryFileNamesPadding.add(SQL_ROWNUMBER_EQUAL_N_PADDING_FILENAME);
        queryFileNamesPadding.add(SQL_ROWNUMBER_LESS_N_PADDING_FILENAME);
        ArrayList<String> queryFileNamesNoPadding = new ArrayList<String>();
        queryFileNamesNoPadding.add(SQL_ROWNUMBER_EQUAL_1_FILENAME);
        queryFileNamesNoPadding.add(SQL_ROWNUMBER_EQUAL_N_FILENAME);
        queryFileNamesNoPadding.add(SQL_ROWNUMBER_LESS_N_FILENAME);

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

