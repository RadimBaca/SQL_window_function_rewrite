package vsb.baca.sql;

import vsb.baca.sql.benchmark.*;
import vsb.baca.sql.model.Config;

import java.util.ArrayList;
import java.util.logging.*;

public class mysql_runner {

    private static final String CONNECTION_STRING = "jdbc:mysql://bayer.cs.vsb.cz:3306/sqlbench";
    private static final String USERNAME = "sqlbench";
    private static final String PASSWORD = "n3cUmubsbo";

    private static final String PARALLEL_1 = "SET LOCAL innodb_parallel_read_threads=1";
    private static final String PARALLEL_8 = "SET LOCAL innodb_parallel_read_threads=8";

    private static Config config = new Config(Config.dbms.MYSQL, false, true);
    private static Logger logger = Logger.getLogger("MyLogger");
    private static FileHandler fileHandler;

    public static void prepare_run(ArrayList<run_setup> run_setups,
                                    String dropindexesFilename,
                                    String createindexesFilename) throws Exception {
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
            fileHandler = new FileHandler("log_mysql.txt", 1024 * 1024, 1, true);
            logger.addHandler(fileHandler);
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (run_setup rs : run_setups) {
            bench_config_mysql bench_config = null;
            if (rs.parallelism == vsb.baca.sql.benchmark.bench_config.Parallelism.ON) {
                bench_config = new bench_config_mysql(createindexesFilename, dropindexesFilename,
                        CONNECTION_STRING, USERNAME, PASSWORD, config, logger, fileHandler,
                        rs.queryFileNames, rs.tab_prefix, PARALLEL_8,
                        rs.padding, rs.storage, rs.parallelism);
            } else {
                bench_config = new bench_config_mysql(createindexesFilename, dropindexesFilename,
                        CONNECTION_STRING, USERNAME, PASSWORD, config, logger, fileHandler,
                        rs.queryFileNames, rs.tab_prefix, PARALLEL_1,
                        rs.padding, rs.storage, rs.parallelism);
            }

            benchmark_mysql rank_benchmark = new benchmark_mysql(bench_config);
            rank_benchmark.run();
        }

        fileHandler.close();
    }


}
