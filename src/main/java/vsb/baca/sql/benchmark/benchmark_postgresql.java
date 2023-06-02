package vsb.baca.sql.benchmark;

import org.antlr.v4.runtime.misc.Pair;

import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class benchmark_postgresql extends benchmark {
    public benchmark_postgresql(bench_config_postgresql bench_config_postgres) {
        super(bench_config_postgres);
    }

    @Override protected String setUpQuery(String sql) {
        return sql;
    }

    @Override protected Pair<Long, Integer> getQueryProcessingTime(String sql) {
        try (Connection connection = DriverManager.getConnection(bconfig.CONNECTION_STRING, bconfig.USERNAME, bconfig.PASSWORD);
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(60);
            statement.execute(((bench_config_postgresql)bconfig).SET_PARALLEL_WORKERS);
            statement.execute(((bench_config_postgresql)bconfig).SET_PARALLEL_WORKERS_PER_GATHER);

            // Execute the query
            sql = "explain analyse " + sql;
            ResultSet resultSet = statement.executeQuery(sql);

            String queryPlan = "";
            while (resultSet.next()) {
                queryPlan = resultSet.getString("QUERY PLAN");
            }
            resultSet.close();

            bconfig.logger.info(queryPlan);
            String regex = "Execution Time: (\\d+\\.\\d+) ms";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(queryPlan);
            if (matcher.find()) {
                double elapsedTime = Double.parseDouble(matcher.group(1));
                return new Pair((long) elapsedTime, 0);
            }
            throw new RuntimeException("Actual time not found in query plan!");

        }
        catch (SQLTimeoutException e) {
//            System.out.println("Query timed out!");
            return new Pair((long)60000, -1);
        }
        catch (SQLException e) {
            if (e.getMessage().contains("ERROR: canceling statement due to statement timeout")) {
                return new Pair((long)60000, -1);
            }
            e.printStackTrace();
        }
        return new Pair((long)60000, -1);
    }

    @Override protected String compileResultRow(long sql1_query_time, long sql2_query_time, String index, int B_count, int result_size, bench_config bconfig, String query)
    {
        return sql1_query_time + "," + sql2_query_time + "," + B_count + "," + result_size + "," +
                bconfig.storage.toString() + "," + index + ",padding_" + bconfig.padding.toString() +
                ",parallel_" + bconfig.parallelism.toString() + "," + query;
    }

}
