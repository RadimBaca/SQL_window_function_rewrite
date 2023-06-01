package vsb.baca.sql.benchmark;

import vsb.baca.sql.model.Config;

import java.util.ArrayList;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class bench_config_mssql extends bench_config {
    public String OPTION_MAXDOP;


    public bench_config_mssql(String SQL_RANK_TEST_ROWNUMBER_CREATEINDEXES_SQL,
                              String SQL_RANK_TEST_ROWNUMBER_DROPINDEXES_SQL,
                              String CONNECTION_STRING,
                              Config config,
                              Logger logger,
                              FileHandler fileHandler,
                              ArrayList<String> queryFileNames,
                              String tab_prefix,
                              String OPTION_MAXDOP,
                              Padding padding,
                              Storage storage,
                              Parallelism parallelism) {
        super(SQL_RANK_TEST_ROWNUMBER_CREATEINDEXES_SQL,
                SQL_RANK_TEST_ROWNUMBER_DROPINDEXES_SQL,
                CONNECTION_STRING,
                config,
                logger,
                fileHandler,
                queryFileNames,
                tab_prefix,
                storage,
                padding,
                parallelism);
        this.OPTION_MAXDOP = OPTION_MAXDOP;
    }
}
