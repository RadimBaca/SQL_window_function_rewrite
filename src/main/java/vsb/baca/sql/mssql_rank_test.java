package vsb.baca.sql;


import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.tree.ParseTree;
import vsb.baca.grammar.Mssql;
import vsb.baca.grammar.Mssql_lexer;
import vsb.baca.grammar.rewriter.Mssql_rewriter_visitor;
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

public class mssql_rank_test {

//    private static final String TAB = "R";
//    private static final String SQL_RANK_TEST_ROWNUMBER_EQUAL_1_SQL = "sql/rank_test/rownumber_equal1.sql";
//    private static final String SQL_RANK_TEST_ROWNUMBER_EQUAL_N_SQL = "sql/rank_test/rownumber_equalN.sql";
//    private static final String SQL_RANK_TEST_ROWNUMBER_LESS_N_SQL = "sql/rank_test/rownumber_lessN.sql";
//    private static final String OPTION_MAXDOP_1 = "\nOPTION(MAXDOP 1)";

//    private static final String TAB = "S";
//    private static final String SQL_RANK_TEST_ROWNUMBER_EQUAL_1_SQL = "sql/rank_test/rownumber_equal1_padding.sql";
//    private static final String SQL_RANK_TEST_ROWNUMBER_EQUAL_N_SQL = "sql/rank_test/rownumber_equalN_padding.sql";
//    private static final String SQL_RANK_TEST_ROWNUMBER_LESS_N_SQL = "sql/rank_test/rownumber_lessN_padding.sql";
//    private static final String OPTION_MAXDOP_1 = "\nOPTION(MAXDOP 1)";

//    private static final String TAB = "S";
//    private static final String SQL_RANK_TEST_ROWNUMBER_EQUAL_1_SQL = "sql/rank_test/rownumber_equal1_padding.sql";
//    private static final String SQL_RANK_TEST_ROWNUMBER_EQUAL_N_SQL = "sql/rank_test/rownumber_equalN_padding.sql";
//    private static final String SQL_RANK_TEST_ROWNUMBER_LESS_N_SQL = "sql/rank_test/rownumber_lessN_padding.sql";
//    private static final String OPTION_MAXDOP_1 = "\nOPTION(MAXDOP 8)";

    private static final String TAB = "P_column_";
    private static final String SQL_RANK_TEST_ROWNUMBER_EQUAL_1_SQL = "sql/rank_test/rownumber_equal1_padding.sql";
    private static final String SQL_RANK_TEST_ROWNUMBER_EQUAL_N_SQL = "sql/rank_test/rownumber_equalN_padding.sql";
    private static final String SQL_RANK_TEST_ROWNUMBER_LESS_N_SQL = "sql/rank_test/rownumber_lessN_padding.sql";
    private static final String OPTION_MAXDOP_1 = "\nOPTION(MAXDOP 8)";

    private static final String SQL_RANK_TEST_ROWNUMBER_DROPINDEXES_SQL = "sql/rank_test/rownumber_dropindexes.sql";
    private static final String SQL_RANK_TEST_ROWNUMBER_CREATEINDEXES_SQL = "sql/rank_test/rownumber_createindexes.sql";
    private static final String CONNECTION_STRING = "jdbc:sqlserver://bayer.cs.vsb.cz;instanceName=sqldb;databaseName=sqlbench_window;;user=sqlbench;password=n3cUmubsbo";
    private static Config config = new Config(Config.dbms.MSSQL, false, true);

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
            fileHandler = new FileHandler("log.txt", 1024 * 1024, 1, true);
            logger.addHandler(fileHandler);
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // create a list of table names "R100", "R300", "R1000", "R3000" ...
        ArrayList<Pair<String, Integer>> tableNames = new ArrayList<Pair<String, Integer>>();
        for (int i = 1; i <= 4; i++) {
            // convert pow result into integer
            tableNames.add(new Pair(TAB + (int) pow(10, i), (int) pow(10, i)));
            tableNames.add(new Pair(TAB + 3 * (int) pow(10, i), 3 * (int) pow(10, i)));
        }

        ArrayList<String> createindexes = new ArrayList<String>();
        ArrayList<String> dropindexes = new ArrayList<String>();
        // read create index commands from input file, one command per line
        readDDLCommands(createindexes, SQL_RANK_TEST_ROWNUMBER_CREATEINDEXES_SQL);
        // read drop index commands from input file, one command per line
        readDDLCommands(dropindexes, SQL_RANK_TEST_ROWNUMBER_DROPINDEXES_SQL);

        // run initial drop index commands
        for (int i = 0; i < createindexes.size(); i++) {
            String dropindex = dropindexes.get(i);
            runCommand(dropindex, tableNames);
        }

        for (int i = 0; i < createindexes.size(); i++) {
            String createindex = createindexes.get(i);
            String dropindex = dropindexes.get(i);

            runCommand(createindex, tableNames);

            ArrayList<String> fileNames = new ArrayList<String>();
            fileNames.add(SQL_RANK_TEST_ROWNUMBER_EQUAL_1_SQL);
            fileNames.add(SQL_RANK_TEST_ROWNUMBER_EQUAL_N_SQL);
            fileNames.add(SQL_RANK_TEST_ROWNUMBER_LESS_N_SQL);

            for (String name : fileNames) {

                benchmarkSQLrewrite(name, tableNames);
            }

            runCommand(dropindex, tableNames);
        }

        fileHandler.close();
    }

    private static void readDDLCommands(ArrayList<String> createindexes, String indexFileName) {
        try (FileReader fileReader = new FileReader(indexFileName);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                createindexes.add(line);
            }
        } catch (IOException e) {
            System.out.println("Error reading file: " + e.getMessage());
        }
    }

    private static void runCommand(String sql_index, ArrayList<Pair<String, Integer>> tableNames) {
        System.out.println("----------------------------------------");
        System.out.println("DDL: " + sql_index);
        logger.info("DDL: " + sql_index);
        for (Pair<String, Integer> tableName : tableNames) {
            String cmd = sql_index.replace("tab", tableName.a);
            try (Connection connection = DriverManager.getConnection(CONNECTION_STRING);
                 Statement statement = connection.createStatement()) {
                statement.execute(cmd);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static boolean benchmarkSQLrewrite(String fileName, ArrayList<Pair<String, Integer>> tableNames) {
        String sqlInit = "";
        try (FileReader fileReader = new FileReader(fileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                sqlInit += "\n" + line;
            }
        } catch (IOException e) {
            System.out.println("Error reading file: " + e.getMessage());
        }


        System.out.println("\n" + fileName);
        System.out.println("Groups,SQL1,SQL2");
        logger.info("\n" + fileName);
        for (Pair<String, Integer> tableName : tableNames) {
            String sql1 = sqlInit.replace("TAB", tableName.a);
            String sql2 = rewriteSQL(sql1);
            sql1 += OPTION_MAXDOP_1;
            sql2 += OPTION_MAXDOP_1;
            logger.info("SQL1:\n" + sql1);
            logger.info("SQL2:\n" + sql2);

            // execute the queries three times and get the lowest time
            Pair<Long, Integer> sql1_time = processQueryNTimes(sql1, 3);
            Pair<Long, Integer> sql2_time = processQueryNTimes(sql2, 3 );

            // check if the result sizes are the same
            if (sql1_time.b.compareTo(sql2_time.b) != 0) {
                System.out.println("ERROR: result size mismatch. SQL1: " + sql1_time.b + ", SQL2: " + sql2_time.b);
                logger.info("ERROR: result size mismatch!");
                return false;
            }

            System.out.println(tableName.b + "," + sql1_time.a + "," + sql2_time.a);
            logger.info(tableName.b + "," + sql1_time.a + "," + sql2_time.a);
        }


        return true;
    }

    /**
     * Execute the sql query n times and return the lowest time and result size
     * @param sql sql query
     * @param n number of times to execute the query
     * @return Pair(time, result size)
     */
    private static Pair<Long, Integer> processQueryNTimes(String sql, int n) {
        long lowest_time;
        Pair<Long, Integer> return_pair = null;
        ArrayList<Pair<Long, Integer>> sql_times = new ArrayList<Pair<Long, Integer>>();
        for (int i = 0; i < n; i++) {
            sql_times.add(getQueryProcessingTime(sql));
        }
        // find lowest value in sql_times
        lowest_time = Long.MAX_VALUE;
        for (int i = 0; i < n; i++) {
            if (sql_times.get(i).a < lowest_time) {
                lowest_time = sql_times.get(i).a;
                return_pair = sql_times.get(i);
            }
        }
        return return_pair;
    }

    private static Pair<Long, Integer> getQueryProcessingTime(String sql) {

        try (Connection connection = DriverManager.getConnection(CONNECTION_STRING);
             Statement statement = connection.createStatement()) {

            // Enable statistics
            statement.execute("SET STATISTICS TIME ON");

            // Execute the query
            ResultSet resultSet = statement.executeQuery(sql);

            int resultSize = 0;
            while (resultSet.next()) {
                resultSize++;
            }
            resultSet.close();

            // Check for SQL warnings
            SQLWarning warning = statement.getWarnings();
            while (warning != null) {
                String[] lines = warning.getMessage().split("\n"); // split warning.getMessage() according \n
                if (lines[1].trim().equals("SQL Server Execution Times:")) {
                    Pattern pattern = Pattern.compile("\\d+");
                    Matcher matcher = pattern.matcher(lines[2].trim());

                    if (matcher.find()) {
                        String cpuTime = matcher.group();
                        if (matcher.find()) {
                            String elapsedTime = matcher.group();
                            return new Pair(Long.parseLong(elapsedTime), resultSize);
                        }
                    }
                }
                warning = warning.getNextWarning();
            }
            throw new RuntimeException("No SQL Server Execution Times found in SQL warnings.");

            //return measureUsingSystemCatalog(sql, connection);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new Pair(-1, -1);
    }

    private static long measureUsingSystemCatalog(String sql, Connection connection) throws SQLException {
        // Get the SQL handle for the executed query
        String sqlHandleQuery = "SELECT plan_handle FROM sys.dm_exec_query_stats CROSS APPLY sys.dm_exec_sql_text(sql_handle) WHERE text LIKE ?";
        PreparedStatement handleStatement = connection.prepareStatement(sqlHandleQuery);
        handleStatement.setString(1, sql);
        ResultSet handleResultSet = handleStatement.executeQuery();

        if (handleResultSet.next()) {
            byte[] planHandle = handleResultSet.getBytes("plan_handle");

            // Get the execution time information from sys.dm_exec_query_stats
            String timeQuery = "SELECT last_elapsed_time FROM sys.dm_exec_query_stats WHERE plan_handle = ?";
            PreparedStatement timeStatement = connection.prepareStatement(timeQuery);
            timeStatement.setBytes(1, planHandle);
            ResultSet timeResultSet = timeStatement.executeQuery();

            if (timeResultSet.next()) {
                return (long)timeResultSet.getLong("last_elapsed_time");
            }

            timeResultSet.close();
        }
        handleResultSet.close();
        return -1;
    }

    private static String rewriteSQL(String sql) {
        ANTLRInputStream input = new ANTLRInputStream(sql.toUpperCase());
        Mssql_lexer lexer = new Mssql_lexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        Mssql parser = new Mssql(tokens);

        ParseTree tree = parser.tsql_file(); // begin parsing at init rule
        Mssql_rewriter_visitor visitor = new Mssql_rewriter_visitor();
        visitor.setConfig(config);
        visitor.visit(tree);
        return visitor.getSelectCmd().getQueryText();

    }


}

