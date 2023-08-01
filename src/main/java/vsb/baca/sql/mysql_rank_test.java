package vsb.baca.sql;

import org.antlr.v4.runtime.misc.Pair;
import vsb.baca.sql.benchmark.bench_config;
import vsb.baca.sql.benchmark.benchmark_mssql;
import vsb.baca.sql.benchmark.run_setup;
import vsb.baca.sql.model.Config;

import java.util.ArrayList;

public class mysql_rank_test {

    private static final String SQL_ROWNUMBER_EQUAL_1_PADDING_FILENAME = "sql/rank_test/rownumber_equal1_padding.sql";
    private static final String SQL_ROWNUMBER_EQUAL_N_PADDING_FILENAME = "sql/rank_test/rownumber_equalN_padding.sql";
    private static final String SQL_ROWNUMBER_LESS_N_PADDING_FILENAME = "sql/rank_test/rownumber_lessN_padding.sql";
    private static final String SQL_ROWNUMBER_EQUAL_1_FILENAME = "sql/rank_test/rownumber_equal1.sql";
    private static final String SQL_ROWNUMBER_EQUAL_N_FILENAME = "sql/rank_test/rownumber_equalN.sql";
    private static final String SQL_ROWNUMBER_LESS_N_FILENAME = "sql/rank_test/rownumber_lessN.sql";
    private static ArrayList<run_setup> run_setups = new ArrayList<run_setup>();

    private static final String DROPINDEXES_FILENAME = "sql/rank_test/rownumber_dropindexes_mysql.sql";
    private static final String CREATEINDEXES_FILENAME = "sql/rank_test/rownumber_createindexes.sql";

    private static Config config = new Config(Config.dbms.MYSQL, false, Config.rank_algorithm.BestFit);

    private static String connection_string;
    private static String username;
    private static String password;

    public static void run(String connection_string, String username, String password) throws Exception {
        mysql_rank_test.connection_string = connection_string;
        mysql_rank_test.username = username;
        mysql_rank_test.password = password;

        ArrayList<Pair<String,String>> queryFileNamesPadding = new ArrayList<Pair<String,String>>();
        queryFileNamesPadding.add(benchmark_mssql.readQueryFromFile(SQL_ROWNUMBER_EQUAL_1_PADDING_FILENAME));
        queryFileNamesPadding.add(benchmark_mssql.readQueryFromFile(SQL_ROWNUMBER_EQUAL_N_PADDING_FILENAME));
        queryFileNamesPadding.add(benchmark_mssql.readQueryFromFile(SQL_ROWNUMBER_LESS_N_PADDING_FILENAME));
        ArrayList<Pair<String,String>> queryFileNamesNoPadding = new ArrayList<Pair<String,String>>();
        queryFileNamesNoPadding.add(benchmark_mssql.readQueryFromFile(SQL_ROWNUMBER_EQUAL_1_FILENAME));
        queryFileNamesNoPadding.add(benchmark_mssql.readQueryFromFile(SQL_ROWNUMBER_EQUAL_N_FILENAME));
        queryFileNamesNoPadding.add(benchmark_mssql.readQueryFromFile(SQL_ROWNUMBER_LESS_N_FILENAME));

        run_setups.add(new run_setup("R_row_", queryFileNamesNoPadding, "", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.OFF, config, mysql_rank_test.connection_string, mysql_rank_test.username, mysql_rank_test.password));
        run_setups.add(new run_setup("P_row_", queryFileNamesPadding, "", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.OFF, config, mysql_rank_test.connection_string, mysql_rank_test.username, mysql_rank_test.password));
        run_setups.add(new run_setup("R_row_", queryFileNamesNoPadding,  "", bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.ON, config, mysql_rank_test.connection_string, mysql_rank_test.username, mysql_rank_test.password));
        run_setups.add(new run_setup("P_row_", queryFileNamesPadding,  "", bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.ON, config, mysql_rank_test.connection_string, mysql_rank_test.username, mysql_rank_test.password));

        mysql_runner.prepare_run(run_setups, DROPINDEXES_FILENAME, CREATEINDEXES_FILENAME);
    }

}

