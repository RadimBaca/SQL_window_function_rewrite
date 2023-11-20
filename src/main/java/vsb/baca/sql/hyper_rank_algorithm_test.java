package vsb.baca.sql;


import org.antlr.v4.runtime.misc.Pair;
import vsb.baca.sql.benchmark.bench_config;
import vsb.baca.sql.benchmark.benchmark;
import vsb.baca.sql.benchmark.run_setup;
import vsb.baca.sql.model.Config;

import java.util.ArrayList;

public class hyper_rank_algorithm_test {

    private static final String SQL_ROWNUMBER_EQUAL_1_PADDING_FILENAME = "sql/rank_test/rownumber_equal1_padding.sql";
    private static final String SQL_ROWNUMBER_EQUAL_1_FILENAME = "sql/rank_test/rownumber_equal1.sql";
    private static ArrayList<run_setup> run_setups = new ArrayList<run_setup>();

    private static final String CREATEINDEXES_FILENAME = "sql/rank_test/rownumber_createindexes_hyper.sql";
    private static final String DROPINDEXES_FILENAME = "sql/rank_test/rownumber_dropindexes_hyper.sql";

    private static String connection_string;

    public static void run(String connection_string) throws Exception {

        hyper_rank_algorithm_test.connection_string = connection_string;

        ArrayList<Pair<String,String>> queryFileNamesPadding = new ArrayList<Pair<String,String>>();
        queryFileNamesPadding.add(benchmark.readQueryFromFile(SQL_ROWNUMBER_EQUAL_1_PADDING_FILENAME));
        ArrayList<Pair<String,String>> queryFileNamesNoPadding = new ArrayList<Pair<String,String>>();
        queryFileNamesNoPadding.add(benchmark.readQueryFromFile(SQL_ROWNUMBER_EQUAL_1_FILENAME));

        ArrayList<Config> configs = new ArrayList<Config>();
        configs.add(new Config(Config.dbms.POSTGRESQL, false, Config.rank_algorithm.JoinMin));
        configs.add(new Config(Config.dbms.POSTGRESQL, false, Config.rank_algorithm.LateralDistinctLimit));
        configs.add(new Config(Config.dbms.POSTGRESQL, false, Config.rank_algorithm.LateralLimit));
        configs.add(new Config(Config.dbms.POSTGRESQL, false, Config.rank_algorithm.LateralAgg));

        for (Config config : configs) {
            run_setups.add(new run_setup("R_COLUMN_", queryFileNamesNoPadding, "", bench_config.Padding.OFF, bench_config.Storage.COLUMN, bench_config.Parallelism.ON, config, hyper_rank_algorithm_test.connection_string, null, null));
            run_setups.add(new run_setup("P_COLUMN_", queryFileNamesPadding, "", bench_config.Padding.ON, bench_config.Storage.COLUMN, bench_config.Parallelism.ON, config, hyper_rank_algorithm_test.connection_string, null, null));
        }

        hyper_runner.prepare_run(run_setups, DROPINDEXES_FILENAME, CREATEINDEXES_FILENAME);
    }

}

