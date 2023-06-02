package vsb.baca.sql;


import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.tree.ParseTree;
import vsb.baca.grammar.Mssql;
import vsb.baca.grammar.Mssql_lexer;
import vsb.baca.grammar.rewriter.Mssql_rewriter_visitor;
import vsb.baca.sql.benchmark.*;
import vsb.baca.sql.model.Config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.logging.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.pow;

public class postgre_rank_test {


    private static final String SQL_ROWNUMBER_EQUAL_1_PADDING_FILENAME = "sql/rank_test/rownumber_equal1_padding.sql";
    private static final String SQL_ROWNUMBER_EQUAL_N_PADDING_FILENAME = "sql/rank_test/rownumber_equalN_padding.sql";
    private static final String SQL_ROWNUMBER_LESS_N_PADDING_FILENAME = "sql/rank_test/rownumber_lessN_padding.sql";
    private static final String SQL_ROWNUMBER_EQUAL_1_FILENAME = "sql/rank_test/rownumber_equal1.sql";
    private static final String SQL_ROWNUMBER_EQUAL_N_FILENAME = "sql/rank_test/rownumber_equalN.sql";
    private static final String SQL_ROWNUMBER_LESS_N_FILENAME = "sql/rank_test/rownumber_lessN.sql";
    private static ArrayList<run_setup_postgresql> run_setups = new ArrayList<run_setup_postgresql>();

    private static final String CONNECTION_STRING = "jdbc:postgresql://bayer.cs.vsb.cz:5433/sqlbench";
    private static final String USERNAME = "sqlbench";
    private static final String PASSWORD = "n3cUmubsbo";
    private static final String DROPINDEXES_FILENAME = "sql/rank_test/rownumber_dropindexes_postgre.sql";
    private static final String CREATEINDEXES_FILENAME = "sql/rank_test/rownumber_createindexes.sql";
    private static final String SET_PARALLEL_WORKERS_0 = "SET max_parallel_workers = 0";
    private static final String SET_PARALLEL_WORKERS_PER_GATHER_0 = "SET max_parallel_workers_per_gather = 0";
    private static final String SET_PARALLEL_WORKERS_N = " "; // we allow to use any number of parallel threads
    private static final String SET_PARALLEL_WORKERS_PER_GATHER_N = " "; // we allow to use any number of parallel threads per gather
    private static Config config = new Config(Config.dbms.POSTGRESQL, false, true);
    private static Logger logger = Logger.getLogger("MyLogger");
    private static FileHandler fileHandler;

    public static void main(String[] args) throws Exception {

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

        ArrayList<String> queryFileNamesPadding = new ArrayList<String>();
        queryFileNamesPadding.add(SQL_ROWNUMBER_EQUAL_1_PADDING_FILENAME);
        queryFileNamesPadding.add(SQL_ROWNUMBER_EQUAL_N_PADDING_FILENAME);
        queryFileNamesPadding.add(SQL_ROWNUMBER_LESS_N_PADDING_FILENAME);
        ArrayList<String> queryFileNamesNoPadding = new ArrayList<String>();
        queryFileNamesNoPadding.add(SQL_ROWNUMBER_EQUAL_1_FILENAME);
        queryFileNamesNoPadding.add(SQL_ROWNUMBER_EQUAL_N_FILENAME);
        queryFileNamesNoPadding.add(SQL_ROWNUMBER_LESS_N_FILENAME);

        run_setups.add(new run_setup_postgresql("R_row_", queryFileNamesNoPadding, SET_PARALLEL_WORKERS_0, SET_PARALLEL_WORKERS_PER_GATHER_0, bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.OFF));
        run_setups.add(new run_setup_postgresql("P_row_", queryFileNamesPadding, SET_PARALLEL_WORKERS_0, SET_PARALLEL_WORKERS_PER_GATHER_0, bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.OFF));
        run_setups.add(new run_setup_postgresql("R_row_", queryFileNamesNoPadding,  SET_PARALLEL_WORKERS_N, SET_PARALLEL_WORKERS_PER_GATHER_N, bench_config.Padding.OFF, bench_config.Storage.ROW, bench_config.Parallelism.ON));
        run_setups.add(new run_setup_postgresql("P_row_", queryFileNamesPadding,  SET_PARALLEL_WORKERS_N, SET_PARALLEL_WORKERS_PER_GATHER_N, bench_config.Padding.ON, bench_config.Storage.ROW, bench_config.Parallelism.ON));

        for (run_setup_postgresql run_setup : run_setups) {
            bench_config_postgresql bench_config = new bench_config_postgresql(CREATEINDEXES_FILENAME, DROPINDEXES_FILENAME,
                    CONNECTION_STRING, USERNAME, PASSWORD, config, logger, fileHandler,
                    run_setup.queryFileNames, run_setup.tab_prefix, run_setup.SET_PARALLEL_WORKERS, run_setup.SET_PARALLEL_WORKERS_PER_GATHER, run_setup.padding, run_setup.storage, run_setup.parallelism);

            benchmark_postgresql rank_benchmark = new benchmark_postgresql(bench_config);
            rank_benchmark.run();
        }

        fileHandler.close();
    }

//    private static final String SQL_RANK_TEST_ROWNUMBER_CREATEINDEXES_SQL = "sql/rank_test/rownumber_createindexes.sql";
//    private static final String SQL_RANK_TEST_ROWNUMBER_DROPINDEXES_SQL = "sql/rank_test/rownumber_dropindexes_postgre.sql";
//    private static final String SET_PARALLEL_WORKERS = "SET max_parallel_workers = 2";
//    private static final String SET_PARALLEL_WORKERS_PER_GATHER = "SET max_parallel_workers_per_gather = 2";
//    private static final String CONNECTION_URL = "jdbc:postgresql://158.196.98.67:5432/sqlbench";
//    private static final String USERNAME = "sqlbench";
//    private static final String PASSWORD = "n3cUmubsbo";
//    private static Config config = new Config(Config.dbms.POSTGRESQL, false, true);
//
//    private static Logger logger = Logger.getLogger("MyLogger");
//    private static FileHandler fileHandler;
//
//    public static void main(String[] args) throws Exception {
//
//        // Disable console logging
//        Logger rootLogger = Logger.getLogger("");
//        Handler[] handlers = rootLogger.getHandlers();
//        for (Handler handler : handlers) {
//            if (handler instanceof ConsoleHandler) {
//                rootLogger.removeHandler(handler);
//            }
//        }
//
//        try {
//            // Set up the file path and limit the log file size
//            fileHandler = new FileHandler("log.txt", 1024 * 1024, 1, true);
//            logger.addHandler(fileHandler);
//            SimpleFormatter formatter = new SimpleFormatter();
//            fileHandler.setFormatter(formatter);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        // read SQL from input file
//        String fileName;
//
//        // create a list of table names "R100", "R300", "R1000", "R3000" ...
//        ArrayList<Pair<String, Integer>> tableNames = new ArrayList<Pair<String, Integer>>();
//        for (int i = 1; i <= 4; i++) {
//            // convert pow result into integer
//            tableNames.add(new Pair("R" + (int) pow(10, i), (int) pow(10, i)));
//            tableNames.add(new Pair("R" + 3 * (int) pow(10, i), 3 * (int) pow(10, i)));
//        }
//
//        ArrayList<String> createindexes = new ArrayList<String>();
//        ArrayList<String> dropindexes = new ArrayList<String>();
//        // read create index commands from input file, one command per line
//        readDDLCommands(createindexes, SQL_RANK_TEST_ROWNUMBER_CREATEINDEXES_SQL);
//        // read drop index commands from input file, one command per line
//        readDDLCommands(dropindexes, SQL_RANK_TEST_ROWNUMBER_DROPINDEXES_SQL);
//
//        // run initial drop index commands
//        for (int i = 0; i < createindexes.size(); i++) {
//            String dropindex = dropindexes.get(i);
//            runDDLCommand(dropindex, tableNames);
//        }
//
//        for (int i = 0; i < createindexes.size(); i++) {
//            String createindex = createindexes.get(i);
//            String dropindex = dropindexes.get(i);
//
//            runDDLCommand(createindex, tableNames);
//
//            ArrayList<String> fileNames = new ArrayList<String>();
//            fileNames.add("sql/rank_test/rownumber_equal1.sql");
//            fileNames.add("sql/rank_test/rownumber_equalN.sql");
//            fileNames.add("sql/rank_test/rownumber_lessN.sql");
//
//            for (String name : fileNames) {
//                benchmarkSQLrewrite(name, tableNames);
//            }
//
//            runDDLCommand(dropindex, tableNames);
//        }
//
//        fileHandler.close();
//    }
//
//    private static void readDDLCommands(ArrayList<String> createindexes, String indexFileName) {
//        try (FileReader fileReader = new FileReader(indexFileName);
//             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
//            String line;
//            while ((line = bufferedReader.readLine()) != null) {
//                createindexes.add(line);
//            }
//        } catch (IOException e) {
//            System.out.println("Error reading file: " + e.getMessage());
//        }
//    }
//
//    private static void runDDLCommand(String sql_index, ArrayList<Pair<String, Integer>> tableNames) {
//        System.out.println("----------------------------------------");
//        System.out.println("DDL: " + sql_index);
//        logger.info("DDL: " + sql_index);
//        for (Pair<String, Integer> tableName : tableNames) {
//            String cmd = sql_index.replace("tab", tableName.a);
//            try (Connection connection = DriverManager.getConnection(CONNECTION_URL, USERNAME, PASSWORD);
//                 Statement statement = connection.createStatement()) {
//                logger.info(cmd);
//                statement.execute(cmd);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    private static boolean benchmarkSQLrewrite(String fileName, ArrayList<Pair<String, Integer>> tableNames) {
//        String sqlInit = "";
//        try (FileReader fileReader = new FileReader(fileName);
//            BufferedReader bufferedReader = new BufferedReader(fileReader)) {
//            String line;
//            while ((line = bufferedReader.readLine()) != null) {
//                sqlInit += "\n" + line;
//            }
//        } catch (IOException e) {
//            System.out.println("Error reading file: " + e.getMessage());
//        }
//
//
//        System.out.println("\n" + fileName);
//        System.out.println("Groups,SQL1,SQL2");
//        logger.info("\n" + fileName);
//        for (Pair<String, Integer> tableName : tableNames) {
//            String sql1 = sqlInit.replace("TAB", tableName.a);
//            String sql2 = rewriteSQL(sql1);
//
//            logger.info("SQL1:\n" + sql1);
//            logger.info("SQL2:\n" + sql2);
//
//            // execute the queries three times and print the results
//            Pair<Long, Integer> sql1_time = processQueryNTimes(sql1, 3);
//            Pair<Long, Integer> sql2_time = processQueryNTimes(sql2, 3 );
//
////            if (sql1_time.b.compareTo(sql2_time.b) != 0) {
////                System.out.println("ERROR: result size mismatch. SQL1: " + sql1_time.b + ", SQL2: " + sql2_time.b);
////                logger.info("ERROR: result size mismatch!");
////                return false;
////            }
//
//            System.out.println(tableName.b + "," + sql1_time.a + "," + sql2_time.a);
//            logger.info(tableName.b + "," + sql1_time.a + "," + sql2_time.a);
//        }
//
//
//        return true;
//    }
//
//    /**
//     *
//     * @param sql
//     * @param n
//     * @return Pair(time, result size)
//     */
//    private static Pair<Long, Integer> processQueryNTimes(String sql, int n) {
//        long lowest_time;
//        Pair<Long, Integer> return_pair = null;
//        ArrayList<Pair<Long, Integer>> sql_times = new ArrayList<Pair<Long, Integer>>();
//        for (int i = 0; i < n; i++) {
//            Pair<Long, Integer> result = getQueryProcessingTime(sql);
//            sql_times.add(result);
//            if (result.a.equals((long)60000)) {
//                return result;
//            }
//        }
//        // find lowest value in sql_times
//        lowest_time = Long.MAX_VALUE;
//        for (int i = 0; i < n; i++) {
//            if (sql_times.get(i).a < lowest_time) {
//                lowest_time = sql_times.get(i).a;
//                return_pair = sql_times.get(i);
//            }
//        }
//        return return_pair;
//    }
//
//    private static Pair<Long, Integer> getQueryProcessingTime(String sql) {
//
//        try (Connection connection = DriverManager.getConnection(CONNECTION_URL, USERNAME, PASSWORD);
//             Statement statement = connection.createStatement()) {
//            statement.setQueryTimeout(60);
//            statement.execute(SET_PARALLEL_WORKERS);
//            statement.execute(SET_PARALLEL_WORKERS_PER_GATHER);
//
//            // Execute the query
//            sql = "explain analyse " + sql;
//            ResultSet resultSet = statement.executeQuery(sql);
//
//            String queryPlan = "";
//            while (resultSet.next()) {
//                queryPlan = resultSet.getString("QUERY PLAN");
//            }
//            resultSet.close();
//
//            logger.info(queryPlan);
//            String regex = "Execution Time: (\\d+\\.\\d+) ms";
//            Pattern pattern = Pattern.compile(regex);
//            Matcher matcher = pattern.matcher(queryPlan);
//            if (matcher.find()) {
//                double elapsedTime = Double.parseDouble(matcher.group(1));
//                return new Pair((long) elapsedTime, 0);
//            }
//            throw new RuntimeException("Actual time not found in query plan!");
//
//            //return measureUsingSystemCatalog(sql, connection);
//
//        }
//        catch (SQLTimeoutException e) {
////            System.out.println("Query timed out!");
//            return new Pair((long)60000, -1);
//        }
//        catch (SQLException e) {
//            if (e.getMessage().contains("ERROR: canceling statement due to statement timeout")) {
//                return new Pair((long)60000, -1);
//            }
//            e.printStackTrace();
//        }
//        return new Pair((long)60000, -1);
//    }
//
//    private static long measureUsingSystemCatalog(String sql, Connection connection) throws SQLException {
//        // Get the SQL handle for the executed query
//        String sqlHandleQuery = "SELECT plan_handle FROM sys.dm_exec_query_stats CROSS APPLY sys.dm_exec_sql_text(sql_handle) WHERE text LIKE ?";
//        PreparedStatement handleStatement = connection.prepareStatement(sqlHandleQuery);
//        handleStatement.setString(1, sql);
//        ResultSet handleResultSet = handleStatement.executeQuery();
//
//        if (handleResultSet.next()) {
//            byte[] planHandle = handleResultSet.getBytes("plan_handle");
//
//            // Get the execution time information from sys.dm_exec_query_stats
//            String timeQuery = "SELECT last_elapsed_time FROM sys.dm_exec_query_stats WHERE plan_handle = ?";
//            PreparedStatement timeStatement = connection.prepareStatement(timeQuery);
//            timeStatement.setBytes(1, planHandle);
//            ResultSet timeResultSet = timeStatement.executeQuery();
//
//            if (timeResultSet.next()) {
//                long executionTime = timeResultSet.getLong("last_elapsed_time");
//                return executionTime;
//            }
//
//            timeResultSet.close();
//        }
//        handleResultSet.close();
//        return -1;
//    }
//
//    private static String rewriteSQL(String sql) {
//        ANTLRInputStream input = new ANTLRInputStream(sql);
//        Mssql_lexer lexer = new Mssql_lexer(input);
//        CommonTokenStream tokens = new CommonTokenStream(lexer);
//        Mssql parser = new Mssql(tokens);
//
//        ParseTree tree = parser.tsql_file(); // begin parsing at init rule
//        Mssql_rewriter_visitor visitor = new Mssql_rewriter_visitor();
//        visitor.setConfig(config);
//        visitor.visit(tree);
//        return visitor.getSelectCmd().getQueryText();
//
//    }


}

