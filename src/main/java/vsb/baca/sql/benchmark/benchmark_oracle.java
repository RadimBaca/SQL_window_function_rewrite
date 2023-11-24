package vsb.baca.sql.benchmark;

//import oracle.jdbc.driver.OracleConnection;
import org.antlr.v4.runtime.misc.Pair;

import java.sql.*;
import java.util.ArrayList;

public class benchmark_oracle extends benchmark {
    public benchmark_oracle(bench_config_oracle bench_config_oracle) {
        super(bench_config_oracle);
    }

    @Override protected String setUpQuery(String sql) {
        // TODO paralelization
        String psql = sql.replaceAll("SELECT ", "SELECT " + ((bench_config_oracle) bconfig).PARALLEL + " ");
        return psql;
    }

    @Override protected measured_result getQueryProcessingTime(String sql) {
        int queryTimeout = 300;
        try (Connection connection = DriverManager.getConnection(bconfig.CONNECTION_STRING, bconfig.USERNAME, bconfig.PASSWORD);
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(queryTimeout);

//            // Enable the jdbc.sqltiming property
//            Properties props = new Properties();
//            props.setProperty("oracle.jdbc.trackBindVariables", "false");
//            props.setProperty("oracle.net.disableOob", "true");
//            props.setProperty("oracle.jdbc.Trace", "true");
//            props.setProperty("oracle.jdbc.sqltiming", "true");

//            // Apply the properties to the connection
//            ((OracleConnection) connection).setRemarksReporting(true);
//            ((OracleConnection) connection).setPropertyCycle(100);

            long startTime = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            resultSet.setFetchSize(1000);
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            long endTime = System.currentTimeMillis();
            resultSet.close();

//            System.out.println("Query time: " + (endTime - startTime) + " [ms]");

            return new measured_result(endTime - startTime, count);
        }
        catch (SQLTimeoutException e) {
                return new measured_result((long)queryTimeout * 1000, -1);
        }
        catch (SQLException e) {
            if (e.getErrorCode() == 1013) {
                return new measured_result((long)queryTimeout * 1000, -1);
            } else {
                e.printStackTrace();
            }
        }
        return new measured_result(-1, -1);
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
                    if (e.getMessage().contains("ORA-01418"))
                        continue;
                    e.printStackTrace();
                }
            }

        }
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
