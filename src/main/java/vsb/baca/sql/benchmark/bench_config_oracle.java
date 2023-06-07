package vsb.baca.sql.benchmark;

import org.antlr.v4.runtime.misc.Pair;
import vsb.baca.sql.model.Config;

import java.util.ArrayList;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class bench_config_oracle extends bench_config {
    public String PARALLEL;


    public bench_config_oracle(String SQL_RANK_TEST_ROWNUMBER_CREATEINDEXES_SQL,
                               String SQL_RANK_TEST_ROWNUMBER_DROPINDEXES_SQL,
                               String CONNECTION_STRING,
                               String username,
                               String password,
                               Config config,
                               Logger logger,
                               FileHandler fileHandler,
                               ArrayList<Pair<String, String>> queryFileNames,
                               String tab_prefix,
                               String parallel,
                               Padding padding,
                               Storage storage,
                               Parallelism parallelism) {
        super(SQL_RANK_TEST_ROWNUMBER_CREATEINDEXES_SQL,
                SQL_RANK_TEST_ROWNUMBER_DROPINDEXES_SQL,
                CONNECTION_STRING,
                username,
                password,
                config,
                logger,
                fileHandler,
                queryFileNames,
                tab_prefix,
                storage,
                padding,
                parallelism);
        PARALLEL = parallel;
    }
}
