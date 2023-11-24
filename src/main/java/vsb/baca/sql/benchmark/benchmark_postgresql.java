package vsb.baca.sql.benchmark;

import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class benchmark_postgresql extends benchmark {
    public benchmark_postgresql(bench_config_postgresql bench_config_postgres) {
        super(bench_config_postgres);
    }

    @Override protected String setUpQuery(String sql) {
        return "explain analyse " + sql;
    }

    @Override protected measured_result getQueryProcessingTime(String sql) {
        int queryTimeout = 300;
        try (Connection connection = DriverManager.getConnection(bconfig.CONNECTION_STRING, bconfig.USERNAME, bconfig.PASSWORD);
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(queryTimeout);
            statement.execute(((bench_config_postgresql)bconfig).SET_PARALLEL_WORKERS);
            statement.execute(((bench_config_postgresql)bconfig).SET_PARALLEL_WORKERS_PER_GATHER);

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
                return new measured_result((long) elapsedTime, 0);
            }
            throw new RuntimeException("Actual time not found in query plan!");

        }
        catch (SQLTimeoutException e) {
//            System.out.println("Query timed out!");
            return new measured_result((long)queryTimeout * 1000, -1);
        }
        catch (SQLException e) {
            if (e.getMessage().contains("ERROR: canceling statement due to statement timeout") ||
                    e.getMessage().contains("ERROR: canceling statement due to user request")) {
                return new measured_result((long)queryTimeout * 1000, -1);
            }
            e.printStackTrace();
        }
        return new measured_result((long)queryTimeout * 1000, -1);
    }

    @Override protected String compileResultRow(measured_result sql1, measured_result sql2, String index, int B_count, bench_config bconfig, String query)
    {
        return sql1.querytime + "," + sql2.querytime + "," + B_count + "," + sql1.resultsize + "," +
                bconfig.storage.toString() + "," + index + ",padding_" + bconfig.padding.toString() +
                ",parallel_" + bconfig.parallelism.toString() + "," + bconfig.config.getSelectedRankAlgorithm().toString() +
                "," + query;
    }


    @Override protected String compileResultRowHeader() {
        return "sql_window_query_time,sql_selfjoin_query_time,B_count,result_size,storage,index,padding,parallel,rank_algorithm,query";
    }
}
