package vsb.baca.sql.benchmark;

import org.antlr.v4.runtime.misc.Pair;

import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class benchmark_mssql extends benchmark {
    public benchmark_mssql(bench_config_mssql bench_config_mssql) {
        super(bench_config_mssql);
    }

    @Override protected String setUpQuery(String sql) {
        return sql + ((bench_config_mssql)bconfig).OPTION_MAXDOP;
    }

    @Override protected Pair<Long, Integer> getQueryProcessingTime(String sql) {
        int queryTimeout = 300;
        try (Connection connection = DriverManager.getConnection(bconfig.CONNECTION_STRING);
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(queryTimeout);
            statement.execute("SET STATISTICS TIME ON");
            ResultSet resultSet = statement.executeQuery(sql);

            int resultSize = 0;
            while (resultSet.next()) {
                resultSize++;
            }
            resultSet.close();

            // read the query processing time
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

        }
        catch (SQLTimeoutException e) {
            return new Pair((long)queryTimeout * 1000, -1);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return new Pair(-1, -1);
    }

    @Override protected String compileResultRow(long sql1_query_time, long sql2_query_time, String index, int B_count, int result_size, bench_config bconfig, String query)
    {
        return sql1_query_time + "," + sql2_query_time + "," + B_count + "," + result_size + "," +
                bconfig.storage.toString() + "," + index + ",padding_" + bconfig.padding.toString() +
                ",parallel_" + bconfig.parallelism.toString() + "," + bconfig.config.getSelectedRankAlgorithm().toString() +
                "," + query;
    }

    // Not used
    private long measureUsingSystemCatalog(String sql, Connection connection) throws SQLException {
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
}
