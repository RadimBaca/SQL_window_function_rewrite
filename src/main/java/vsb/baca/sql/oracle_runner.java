package vsb.baca.sql;

import vsb.baca.sql.benchmark.*;
import vsb.baca.sql.model.Config;

import java.util.ArrayList;
import java.util.logging.*;

public class oracle_runner {

    private static final String PARALLEL_0 = " /*+ no_parallel */ ";
    private static final String PARALLEL_8 = " /*+ PARALLEL(8) */ ";

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
            fileHandler = new FileHandler("log_oracle.txt", 1024 * 1024, 1, true);
            logger.addHandler(fileHandler);
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (run_setup rs : run_setups) {
            bench_config_oracle bench_config = null;
            if (rs.parallelism == vsb.baca.sql.benchmark.bench_config.Parallelism.ON) {
                bench_config = new bench_config_oracle(createindexesFilename, dropindexesFilename,
                        rs.connection_string, rs.username, rs.password, rs.config, logger, fileHandler,
                        rs.queryFileNames, rs.tab_prefix, PARALLEL_8,
                        rs.padding, rs.storage, rs.parallelism);
            } else {
                bench_config = new bench_config_oracle(createindexesFilename, dropindexesFilename,
                        rs.connection_string, rs.username, rs.password, rs.config, logger, fileHandler,
                        rs.queryFileNames, rs.tab_prefix, PARALLEL_0,
                        rs.padding, rs.storage, rs.parallelism);
            }

            benchmark_oracle rank_benchmark = new benchmark_oracle(bench_config);
            rank_benchmark.run();
        }

        fileHandler.close();
    }


}
