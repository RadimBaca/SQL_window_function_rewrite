package vsb.baca.sql;

import vsb.baca.sql.benchmark.*;
import vsb.baca.sql.model.Config;

import java.util.ArrayList;
import java.util.logging.*;

public class mssql_runner {

    private static final String CONNECTION_STRING = "jdbc:sqlserver://bayer.cs.vsb.cz;instanceName=sqldb;databaseName=sqlbench_window;;user=sqlbench;password=n3cUmubsbo";
    private static final String USERNAME = "";
    private static final String PASSWORD = "";
    private static final String MAXDOP_1 = "\nOPTION(MAXDOP 1)";
    private static final String MAXDOP_8 = "\nOPTION(MAXDOP 8)";

    private static Config config = new Config(Config.dbms.MSSQL, false, false);
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
            fileHandler = new FileHandler("log_mssql.txt", 1024 * 1024, 1, true);
            logger.addHandler(fileHandler);
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (run_setup rs : run_setups) {
            bench_config_mssql bench_config = null;
            if (rs.parallelism == vsb.baca.sql.benchmark.bench_config.Parallelism.ON) {
                bench_config = new bench_config_mssql(createindexesFilename, dropindexesFilename,
                        CONNECTION_STRING, USERNAME, PASSWORD, config, logger, fileHandler,
                        rs.queryFileNames, rs.tab_prefix, MAXDOP_8,
                        rs.padding, rs.storage, rs.parallelism);
            } else {
                bench_config = new bench_config_mssql(createindexesFilename, dropindexesFilename,
                        CONNECTION_STRING, USERNAME, PASSWORD, config, logger, fileHandler,
                        rs.queryFileNames, rs.tab_prefix, MAXDOP_1,
                        rs.padding, rs.storage, rs.parallelism);
            }

            benchmark_mssql rank_benchmark = new benchmark_mssql(bench_config);
            rank_benchmark.run();
        }

        fileHandler.close();
    }


}
