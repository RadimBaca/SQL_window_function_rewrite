package vsb.baca.sql;

import vsb.baca.sql.benchmark.bench_config;
import vsb.baca.sql.benchmark.bench_config_postgresql;
import vsb.baca.sql.benchmark.benchmark_postgresql;
import vsb.baca.sql.benchmark.run_setup;
import vsb.baca.sql.model.Config;

import java.util.ArrayList;
import java.util.logging.*;

public class postgre_runner {

    private static final String SET_PARALLEL_WORKERS_0 = "SET max_parallel_workers = 0";
    private static final String SET_PARALLEL_WORKERS_PER_GATHER_0 = "SET max_parallel_workers_per_gather = 0";
    private static final String SET_PARALLEL_WORKERS_N = "SET max_parallel_workers = 8"; // we allow to use any number of parallel threads
    private static final String SET_PARALLEL_WORKERS_PER_GATHER_N = "SET max_parallel_workers_per_gather = 8"; // we allow to use any number of parallel threads per gather

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
            fileHandler = new FileHandler("log_postgresql.txt", 1024 * 1024, 1, true);
            logger.addHandler(fileHandler);
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (run_setup rs : run_setups) {
            bench_config_postgresql bench_config = null;
            if (rs.parallelism == vsb.baca.sql.benchmark.bench_config.Parallelism.ON) {
                bench_config = new bench_config_postgresql(createindexesFilename, dropindexesFilename,
                        rs.connection_string, rs.username, rs.password, rs.config, logger, fileHandler,
                        rs.queryFileNames, rs.tab_prefix, SET_PARALLEL_WORKERS_N, SET_PARALLEL_WORKERS_PER_GATHER_N,
                        rs.padding, rs.storage, rs.parallelism);
            } else {
                bench_config = new bench_config_postgresql(createindexesFilename, dropindexesFilename,
                        rs.connection_string, rs.username, rs.password, rs.config, logger, fileHandler,
                        rs.queryFileNames, rs.tab_prefix, SET_PARALLEL_WORKERS_0, SET_PARALLEL_WORKERS_PER_GATHER_0,
                        rs.padding, rs.storage, rs.parallelism);
            }

            benchmark_postgresql rank_benchmark = new benchmark_postgresql(bench_config);
            rank_benchmark.run();
        }

        fileHandler.close();
    }


}
