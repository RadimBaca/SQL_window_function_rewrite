package vsb.baca.sql.benchmark;

//import oracle.jdbc.driver.OracleConnection;
import org.antlr.v4.runtime.misc.Pair;

import java.sql.*;
import java.util.ArrayList;

public class benchmark_mysql extends benchmark {
    public benchmark_mysql(bench_config_mysql bench_config_mysql) {
        super(bench_config_mysql);
    }

    @Override protected String setUpQuery(String sql) {
        return sql;
    }

    @Override protected Pair<Long, Integer> getQueryProcessingTime(String sql) {
        int queryTimeout = 300;
        try (Connection connection = DriverManager.getConnection(bconfig.CONNECTION_STRING, bconfig.USERNAME, bconfig.PASSWORD);
             Statement statement = connection.createStatement()) {

            statement.setQueryTimeout(queryTimeout);
            statement.execute(((bench_config_mysql)bconfig).PARALLEL);

            long startTime = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            long endTime = System.currentTimeMillis();
            resultSet.close();

            return new Pair(endTime - startTime, count);
        } catch (SQLTimeoutException e) {
//            System.out.println("Query timed out!");
            return new Pair((long)queryTimeout * 1000, -1);
        }
        catch (SQLException e) {
            if (e.getMessage().contains("ERROR: canceling statement due to statement timeout") ||
                    e.getMessage().contains("ERROR: canceling statement due to user request")) {
                return new Pair((long)queryTimeout * 1000, -1);
            }
            e.printStackTrace();
        }
        return new Pair((long)queryTimeout * 1000, -1);
    }


    @Override protected void runCommand(String sql_index, ArrayList<Pair<String, Integer>> tableNames) {
        System.out.println("----------------------------------------");
        System.out.println("DDL: " + sql_index);

        if (sql_index.isEmpty()) return;
        bconfig.logger.info("DDL: " + sql_index);
        for (Pair<String, Integer> tableName : tableNames) {
            String replaced_cmd = sql_index.replace("tab", tableName.a);

            // split the raplaced_cmd into multiple commands
            String[] cmds = replaced_cmd.split(";");
            for (String cmd : cmds) {
                if (cmd.trim().isEmpty()) continue;
                try (Connection connection = DriverManager.getConnection(bconfig.CONNECTION_STRING, bconfig.USERNAME, bconfig.PASSWORD);
                     Statement statement = connection.createStatement()) {
                    statement.execute(cmd);
                } catch (SQLException e) {
                    if (e.getErrorCode() == 1091)
                        continue;
                    e.printStackTrace();
                }
            }

        }
    }

    @Override protected String compileResultRow(long sql1_query_time, long sql2_query_time, String index, int B_count, int result_size, bench_config bconfig, String query)
    {
        return sql1_query_time + "," + sql2_query_time + "," + B_count + "," + result_size + "," +
                bconfig.storage.toString() + "," + index + ",padding_" + bconfig.padding.toString() +
                ",parallel_" + bconfig.parallelism.toString() + "," + query;
    }

}