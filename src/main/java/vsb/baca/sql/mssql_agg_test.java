package vsb.baca.sql;


import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.tree.ParseTree;
import vsb.baca.grammar.Mssql;
import vsb.baca.grammar.Mssql_lexer;
import vsb.baca.grammar.rewriter.Mssql_rewriter_visitor;
import vsb.baca.sql.benchmark.bench_config;
import vsb.baca.sql.benchmark.bench_config_mssql;
import vsb.baca.sql.benchmark.benchmark_mssql;
import vsb.baca.sql.benchmark.run_setup;
import vsb.baca.sql.model.Config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.logging.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static vsb.baca.sql.benchmark.benchmark.*;

public class mssql_agg_test {

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

    private static ArrayList<run_setup> run_setups = new ArrayList<run_setup>();

    private static final String DROPINDEXES_FILENAME = "sql/agg_test/agg_dropindexes.sql";
    private static final String CREATEINDEXES_FILENAME = "sql/agg_test/agg_createindexes.sql";
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

        run_setups.add(new run_setup("R_row_", queryFileNamesNoPadding, "\nOPTION(MAXDOP 1)", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.OFF));
        run_setups.add(new run_setup("R_column_", queryFileNamesNoPadding, "\nOPTION(MAXDOP 1)", bench_config.Padding.OFF, bench_config.Storage.COLUMN, bench_config.Parallelism.OFF));
        run_setups.add(new run_setup("P_row_", queryFileNamesPadding, "\nOPTION(MAXDOP 1)", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.OFF));
        run_setups.add(new run_setup("P_column_", queryFileNamesPadding, "\nOPTION(MAXDOP 1)", bench_config.Padding.ON, bench_config.Storage.COLUMN, bench_config.Parallelism.OFF));
        run_setups.add(new run_setup("R_row_", queryFileNamesNoPadding, "\nOPTION(MAXDOP 8)", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.ON));
        run_setups.add(new run_setup("R_column_", queryFileNamesNoPadding, "\nOPTION(MAXDOP 8)", bench_config.Padding.OFF, bench_config.Storage.COLUMN, bench_config.Parallelism.ON));
        run_setups.add(new run_setup("P_row_", queryFileNamesPadding, "\nOPTION(MAXDOP 8)", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.ON));
        run_setups.add(new run_setup("P_column_", queryFileNamesPadding, "\nOPTION(MAXDOP 8)", bench_config.Padding.ON, bench_config.Storage.COLUMN, bench_config.Parallelism.ON));

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

