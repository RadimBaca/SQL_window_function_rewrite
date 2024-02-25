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

    private static final String COUNT_PB_OB_TEMP = "sql/agg_test/count_pb_ob_temp.sql";
    private static final String COUNT_PB_OB_TEMP_PADDING = "sql/agg_test/count_pb_ob_temp_padding.sql";
    private static final String DROPINDEXES_PB_OB = "sql/agg_test/agg_pb_ob_dropindexes_mssql.sql";
    private static final String CREATEINDEXES_PB_OB = "sql/agg_test/agg_pb_ob_createindexes.sql";

    private static final String COUNT_PB_TEMP = "sql/agg_test/count_pb_temp.sql";
    private static final String COUNT_PB_TEMP_PADDING = "sql/agg_test/count_pb_temp_padding.sql";
    private static final String DROPINDEXES_PB = "sql/agg_test/agg_pb_dropindexes_mssql.sql";
    private static final String CREATEINDEXES_PB = "sql/agg_test/agg_pb_createindexes.sql";

    private static final String MIN_PB_OB_TEMP = "sql/agg_test/min_pb_ob_temp.sql";
    private static final String MIN_PB_OB_TEMP_PADDING = "sql/agg_test/min_pb_ob_temp_padding.sql";

    private static final String MIN_PB_TEMP = "sql/agg_test/min_pb_temp.sql";
    private static final String MIN_PB_TEMP_PADDING = "sql/agg_test/min_pb_temp_padding.sql";

    private static Config config = new Config(Config.dbms.MSSQL, false, Config.rank_algorithm.BestFit);

    private static String connection_string;
    private static String username;
    private static String password;
    private static String storage;

    public static void run(String connection_string, String username, String password, String storage) throws Exception {

        mssql_agg_test.connection_string = connection_string;
        mssql_agg_test.username = username;
        mssql_agg_test.password = password;
        mssql_agg_test.storage = storage;

        ////////////////////////////////////////////
        ArrayList<Integer> selectivity = new ArrayList<Integer>();
        int init_sel = 1;
        for (int i = 0; i < 10; i++) {
            selectivity.add(init_sel);
            init_sel *= 2;
        }

        count_bp_ob_test(selectivity);
        count_bp_test(selectivity);
        min_bp_ob_test(selectivity);
        min_bp_test(selectivity);
    }


    private static void count_bp_ob_test(ArrayList<Integer> selectivity) throws Exception {
        ArrayList<Pair<String, String>> queriesNoPadding = benchmark_mssql.generateQueriesWithSelectivity(selectivity, COUNT_PB_OB_TEMP);
        ArrayList<Pair<String, String>> queriesPadding = benchmark_mssql.generateQueriesWithSelectivity(selectivity, COUNT_PB_OB_TEMP_PADDING);
        ArrayList<run_setup> run_setups = generate_run_setups(queriesNoPadding, queriesPadding);
        mssql_runner.prepare_run(run_setups, DROPINDEXES_PB_OB, CREATEINDEXES_PB_OB);
    }

    private static void count_bp_test(ArrayList<Integer> selectivity) throws Exception {
        ArrayList<Pair<String, String>> queriesNoPadding = benchmark_mssql.generateQueriesWithSelectivity(selectivity, COUNT_PB_TEMP);
        ArrayList<Pair<String, String>> queriesPadding = benchmark_mssql.generateQueriesWithSelectivity(selectivity, COUNT_PB_TEMP_PADDING);
        ArrayList<run_setup> run_setups = generate_run_setups(queriesNoPadding, queriesPadding);
        mssql_runner.prepare_run(run_setups, DROPINDEXES_PB, CREATEINDEXES_PB);
    }

    private static void min_bp_ob_test(ArrayList<Integer> selectivity) throws Exception {
        ArrayList<Pair<String, String>> queriesNoPadding = benchmark_mssql.generateQueriesWithSelectivity(selectivity, MIN_PB_OB_TEMP);
        ArrayList<Pair<String, String>> queriesPadding = benchmark_mssql.generateQueriesWithSelectivity(selectivity, MIN_PB_OB_TEMP_PADDING);
        ArrayList<run_setup> run_setups = generate_run_setups(queriesNoPadding, queriesPadding);
        mssql_runner.prepare_run(run_setups, DROPINDEXES_PB_OB, CREATEINDEXES_PB_OB);
    }

    private static void min_bp_test(ArrayList<Integer> selectivity) throws Exception {
        ArrayList<Pair<String, String>> queriesNoPadding = benchmark_mssql.generateQueriesWithSelectivity(selectivity, MIN_PB_TEMP);
        ArrayList<Pair<String, String>> queriesPadding = benchmark_mssql.generateQueriesWithSelectivity(selectivity, MIN_PB_TEMP_PADDING);
        ArrayList<run_setup> run_setups = generate_run_setups(queriesNoPadding, queriesPadding);
        mssql_runner.prepare_run(run_setups, DROPINDEXES_PB_OB, CREATEINDEXES_PB_OB);
    }


    private static ArrayList<run_setup> generate_run_setups(ArrayList<Pair<String, String>> queriesNoPadding, ArrayList<Pair<String, String>> queriesPadding) {
        ArrayList<run_setup> run_setups = new ArrayList<run_setup>();
        if (mssql_agg_test.storage.contains("row")) {
            run_setups.add(new run_setup("R_row_", queriesNoPadding, "", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.OFF, config, mssql_agg_test.connection_string, mssql_agg_test.username, mssql_agg_test.password));
            run_setups.add(new run_setup("P_row_", queriesPadding, "", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.OFF, config, mssql_agg_test.connection_string, mssql_agg_test.username, mssql_agg_test.password));
            run_setups.add(new run_setup("R_row_", queriesNoPadding, "", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.ON, config, mssql_agg_test.connection_string, mssql_agg_test.username, mssql_agg_test.password));
            run_setups.add(new run_setup("P_row_", queriesPadding, "", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.ON, config, mssql_agg_test.connection_string, mssql_agg_test.username, mssql_agg_test.password));
        }
        if (mssql_agg_test.storage.contains("column")) {
            run_setups.add(new run_setup("R_column_", queriesNoPadding, "", bench_config.Padding.OFF, bench_config.Storage.COLUMN, bench_config.Parallelism.OFF, config, mssql_agg_test.connection_string, mssql_agg_test.username, mssql_agg_test.password));
            run_setups.add(new run_setup("P_column_", queriesPadding, "", bench_config.Padding.ON, bench_config.Storage.COLUMN, bench_config.Parallelism.OFF, config, mssql_agg_test.connection_string, mssql_agg_test.username, mssql_agg_test.password));
            run_setups.add(new run_setup("R_column_", queriesNoPadding, "", bench_config.Padding.OFF, bench_config.Storage.COLUMN, bench_config.Parallelism.ON, config, mssql_agg_test.connection_string, mssql_agg_test.username, mssql_agg_test.password));
            run_setups.add(new run_setup("P_column_", queriesPadding, "", bench_config.Padding.ON, bench_config.Storage.COLUMN, bench_config.Parallelism.ON, config, mssql_agg_test.connection_string, mssql_agg_test.username, mssql_agg_test.password));
        }
        return run_setups;
    }
}

