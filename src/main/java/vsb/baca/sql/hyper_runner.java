package vsb.baca.sql;

import vsb.baca.sql.benchmark.*;
import vsb.baca.sql.benchmark.run_setup;

import java.util.ArrayList;
import java.util.logging.*;

public class hyper_runner {


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
            fileHandler = new FileHandler("log_hyper.txt", 1024 * 1024, 1, true);
            logger.addHandler(fileHandler);
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (run_setup rs : run_setups) {
            bench_config_hyper bench_config = null;
            if (rs.parallelism == vsb.baca.sql.benchmark.bench_config.Parallelism.ON) {
                bench_config = new bench_config_hyper(createindexesFilename, dropindexesFilename,
                        rs.connection_string, rs.config, logger, fileHandler,
                        rs.queryFileNames, rs.tab_prefix, " ",
                        rs.padding, rs.storage, rs.parallelism);
            } else {
                bench_config = new bench_config_hyper(createindexesFilename, dropindexesFilename,
                        rs.connection_string, rs.config, logger, fileHandler,
                        rs.queryFileNames, rs.tab_prefix, " ",
                        rs.padding, rs.storage, rs.parallelism);
            }

            benchmark_hyper rank_benchmark = new benchmark_hyper(bench_config);
            rank_benchmark.run();
        }

        fileHandler.close();
    }


}
