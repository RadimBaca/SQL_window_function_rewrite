package vsb.baca.sql.benchmark;

import com.tableau.hyperapi.Connection;
import com.tableau.hyperapi.HyperProcess;
import com.tableau.hyperapi.Result;
import com.tableau.hyperapi.Telemetry;
import org.antlr.v4.runtime.misc.Pair;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class benchmark_hyper extends benchmark {
    public benchmark_hyper(bench_config_hyper bench_config_hyper) {
        super(bench_config_hyper);
    }

    @Override protected String setUpQuery(String sql) {
        // TODO solve paralelization
        return sql;
    }

    @Override protected Pair<Long, Integer> getQueryProcessingTime(String sql) {
        int queryTimeout = 300;
//        Path pathToDatabase = resolveExampleFile("bench.hyper");
        Path pathToDatabase = resolveExampleFile(((bench_config_hyper)bconfig).hyperFile);

        try (HyperProcess process = new HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU);
             com.tableau.hyperapi.Connection connection = new com.tableau.hyperapi.Connection(
                     process.getEndpoint(),
                     pathToDatabase.toString())) {

            long startTime = System.currentTimeMillis();
            Result result = connection.executeQuery(sql);
            int count = 0;
            while (result.nextRow()) {
                count++;
            }
            long endTime = System.currentTimeMillis();

            return new Pair(endTime - startTime, count);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return new Pair(-1, -1);
    }


    @Override protected void runCommand(String sql_index, ArrayList<Pair<String, Integer>> tableNames) {
        System.out.println("----------------------------------------");
        System.out.println("DDL: " + sql_index + " - statement ignored ");
//
//        if (sql_index.isEmpty()) return;
//        bconfig.logger.info("DDL: " + sql_index);
//        for (Pair<String, Integer> tableName : tableNames) {
//            String replaced_cmd = sql_index.replace("tab", tableName.a);
//
//            // split the raplaced_cmd into multiple commands
//            String[] cmds = replaced_cmd.split(";");
//            for (String cmd : cmds) {
//                if (cmd.trim().isEmpty()) continue;
//                try (Connection connection = DriverManager.getConnection(bconfig.CONNECTION_STRING, bconfig.USERNAME, bconfig.PASSWORD);
//                     Statement statement = connection.createStatement()) {
//                    statement.execute(cmd);
//                } catch (SQLException e) {
//                    if (e.getMessage().contains("ORA-01418"))
//                        continue;
//                    e.printStackTrace();
//                }
//            }
//
//        }
    }

    @Override protected String compileResultRow(long sql1_query_time, long sql2_query_time, String index, int B_count, int result_size, bench_config bconfig, String query)
    {
        return sql1_query_time + "," + sql2_query_time + "," + B_count + "," + result_size + "," +
                bconfig.storage.toString() + "," + index + ",padding_" + bconfig.padding.toString() +
                ",parallel_" + bconfig.parallelism.toString()+ "," + bconfig.config.getSelectedRankAlgorithm().toString() +
                "," + query;
    }

    @Override protected String compileResultRowHeader() {
        return "sql_window_query_time,sql_selfjoin_query_time,B_count,result_size,storage,index,padding,parallel,rank_algorithm,query";
    }

    /**
     * Resolve the example file
     *
     * @param filename The filename
     * @return A path to the resolved file
     */
    private static Path resolveExampleFile(String filename) {
        for (Path path = Paths.get(System.getProperty("user.dir")).toAbsolutePath(); path != null; path = path.getParent()) {
            Path file = path.resolve("data/" + filename);
            if (Files.isRegularFile(file)) {
                return file;
            }
        }
        throw new IllegalAccessError("Could not find example file. Check the working directory.");
    }
}
