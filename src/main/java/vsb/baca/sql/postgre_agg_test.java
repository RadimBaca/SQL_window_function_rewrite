package vsb.baca.sql;


import org.antlr.v4.runtime.misc.Pair;
import vsb.baca.sql.benchmark.*;
import vsb.baca.sql.model.Config;

import java.util.ArrayList;

public class postgre_agg_test {

    private static final String COUNT_PB_OB_TEMP = "sql/agg_test/count_pb_ob_temp.sql";
    private static final String COUNT_PB_OB_TEMP_PADDING = "sql/agg_test/count_pb_ob_temp_padding.sql";
    private static final String DROPINDEXES_PB_OB = "sql/agg_test/agg_pb_ob_dropindexes_postgre.sql";
    private static final String CREATEINDEXES_PB_OB = "sql/agg_test/agg_pb_ob_createindexes.sql";

    private static final String COUNT_PB_TEMP = "sql/agg_test/count_pb_temp.sql";
    private static final String COUNT_PB_TEMP_PADDING = "sql/agg_test/count_pb_temp_padding.sql";
    private static final String DROPINDEXES_PB = "sql/agg_test/agg_pb_dropindexes_postgre.sql";
    private static final String CREATEINDEXES_PB = "sql/agg_test/agg_pb_createindexes.sql";

    private static final String MIN_PB_OB_TEMP = "sql/agg_test/min_pb_ob_temp.sql";
    private static final String MIN_PB_OB_TEMP_PADDING = "sql/agg_test/min_pb_ob_temp_padding.sql";

    private static final String MIN_PB_TEMP = "sql/agg_test/min_pb_temp.sql";
    private static final String MIN_PB_TEMP_PADDING = "sql/agg_test/min_pb_temp_padding.sql";

    private static Config config = new Config(Config.dbms.POSTGRESQL, false, Config.rank_algorithm.BestFit);

    private static String connection_string;
    private static String username;
    private static String password;

    public static void run(String connection_string, String username, String password) throws Exception {
        postgre_agg_test.connection_string = connection_string;
        postgre_agg_test.username = username;
        postgre_agg_test.password = password;

        ////////////////////////////////////////////
        ArrayList<Integer> selectivity = new ArrayList<Integer>();
        int init_sel = 1;
//        for (int i = 0; i < 12; i++) {
        for (int i = 0; i < 6; i++) {
            selectivity.add(init_sel);
            init_sel *= 2;
        }

        count_bp_ob_test(selectivity);
        count_bp_test(selectivity);
        min_bp_ob_test(selectivity);
        min_bp_test(selectivity);
    }


    private static void count_bp_ob_test(ArrayList<Integer> selectivity) throws Exception {
        ArrayList<Pair<String, String>> queriesNoPadding = benchmark_postgresql.generateQueriesWithSelectivity(selectivity, COUNT_PB_OB_TEMP);
        ArrayList<Pair<String, String>> queriesPadding = benchmark_postgresql.generateQueriesWithSelectivity(selectivity, COUNT_PB_OB_TEMP_PADDING);

        ArrayList<run_setup> run_setups = new ArrayList<run_setup>();
        run_setups.add(new run_setup("R_ROW_", queriesNoPadding, "", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.OFF, config, postgre_agg_test.connection_string, postgre_agg_test.username, postgre_agg_test.password));
        run_setups.add(new run_setup("P_ROW_", queriesPadding, "", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.OFF, config, postgre_agg_test.connection_string, postgre_agg_test.username, postgre_agg_test.password));
        run_setups.add(new run_setup("R_ROW_", queriesNoPadding,  "", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.ON, config, postgre_agg_test.connection_string, postgre_agg_test.username, postgre_agg_test.password));
        run_setups.add(new run_setup("P_ROW_", queriesPadding,  "", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.ON, config, postgre_agg_test.connection_string, postgre_agg_test.username, postgre_agg_test.password));

        postgre_runner.prepare_run(run_setups, DROPINDEXES_PB_OB, CREATEINDEXES_PB_OB);
    }

    private static void count_bp_test(ArrayList<Integer> selectivity) throws Exception {
        ArrayList<Pair<String, String>> queriesNoPadding = benchmark_postgresql.generateQueriesWithSelectivity(selectivity, COUNT_PB_TEMP);
        ArrayList<Pair<String, String>> queriesPadding = benchmark_postgresql.generateQueriesWithSelectivity(selectivity, COUNT_PB_TEMP_PADDING);

        ArrayList<run_setup> run_setups = new ArrayList<run_setup>();
        run_setups.add(new run_setup("R_ROW_", queriesNoPadding, "", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.OFF, config, postgre_agg_test.connection_string, postgre_agg_test.username, postgre_agg_test.password));
        run_setups.add(new run_setup("P_ROW_", queriesPadding, "", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.OFF, config, postgre_agg_test.connection_string, postgre_agg_test.username, postgre_agg_test.password));
        run_setups.add(new run_setup("R_ROW_", queriesNoPadding,  "", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.ON, config, postgre_agg_test.connection_string, postgre_agg_test.username, postgre_agg_test.password));
        run_setups.add(new run_setup("P_ROW_", queriesPadding,  "", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.ON, config, postgre_agg_test.connection_string, postgre_agg_test.username, postgre_agg_test.password));

        postgre_runner.prepare_run(run_setups, DROPINDEXES_PB, CREATEINDEXES_PB);
    }

    private static void min_bp_ob_test(ArrayList<Integer> selectivity) throws Exception {
        ArrayList<Pair<String, String>> queriesNoPadding = benchmark_postgresql.generateQueriesWithSelectivity(selectivity, MIN_PB_OB_TEMP);
        ArrayList<Pair<String, String>> queriesPadding = benchmark_postgresql.generateQueriesWithSelectivity(selectivity, MIN_PB_OB_TEMP_PADDING);

        ArrayList<run_setup> run_setups = new ArrayList<run_setup>();
        run_setups.add(new run_setup("R_ROW_", queriesNoPadding, "", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.OFF, config, postgre_agg_test.connection_string, postgre_agg_test.username, postgre_agg_test.password));
        run_setups.add(new run_setup("P_ROW_", queriesPadding, "", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.OFF, config, postgre_agg_test.connection_string, postgre_agg_test.username, postgre_agg_test.password));
        run_setups.add(new run_setup("R_ROW_", queriesNoPadding,  "", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.ON, config, postgre_agg_test.connection_string, postgre_agg_test.username, postgre_agg_test.password));
        run_setups.add(new run_setup("P_ROW_", queriesPadding,  "", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.ON, config, postgre_agg_test.connection_string, postgre_agg_test.username, postgre_agg_test.password));

        postgre_runner.prepare_run(run_setups, DROPINDEXES_PB_OB, CREATEINDEXES_PB_OB);
    }

    private static void min_bp_test(ArrayList<Integer> selectivity) throws Exception {
        ArrayList<Pair<String, String>> queriesNoPadding = benchmark_postgresql.generateQueriesWithSelectivity(selectivity, MIN_PB_TEMP);
        ArrayList<Pair<String, String>> queriesPadding = benchmark_postgresql.generateQueriesWithSelectivity(selectivity, MIN_PB_TEMP_PADDING);

        ArrayList<run_setup> run_setups = new ArrayList<run_setup>();
        run_setups.add(new run_setup("R_ROW_", queriesNoPadding, "", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.OFF, config, postgre_agg_test.connection_string, postgre_agg_test.username, postgre_agg_test.password));
        run_setups.add(new run_setup("P_ROW_", queriesPadding, "", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.OFF, config, postgre_agg_test.connection_string, postgre_agg_test.username, postgre_agg_test.password));
        run_setups.add(new run_setup("R_ROW_", queriesNoPadding,  "", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.ON, config, postgre_agg_test.connection_string, postgre_agg_test.username, postgre_agg_test.password));
        run_setups.add(new run_setup("P_ROW_", queriesPadding,  "", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.ON, config, postgre_agg_test.connection_string, postgre_agg_test.username, postgre_agg_test.password));

        postgre_runner.prepare_run(run_setups, DROPINDEXES_PB_OB, CREATEINDEXES_PB_OB);
    }
}

