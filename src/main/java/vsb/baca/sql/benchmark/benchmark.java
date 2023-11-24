package vsb.baca.sql.benchmark;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.tree.ParseTree;
import vsb.baca.grammar.Mssql;
import vsb.baca.grammar.Mssql_lexer;
import vsb.baca.grammar.rewriter.Mssql_rewriter_visitor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import static java.lang.Math.pow;

public abstract class benchmark {
    bench_config bconfig;
    ArrayList<Pair<String, Integer>> tableNames;
    ArrayList<String> createindexes = new ArrayList<String>();
    ArrayList<String> createindexes_legends = new ArrayList<String>();
    ArrayList<String> dropindexes = new ArrayList<String>();
    ArrayList<Pair<String,String>> queries = new ArrayList<Pair<String,String>>();

    public benchmark(bench_config bench_config) {
        this.bconfig = bench_config;
    }

    protected class measured_result {
        long querytime;
        int resultsize;
        double querycost;

        public measured_result(long a, int b) {
            this.querytime = a;
            this.resultsize = b;
        }

        public measured_result(long a, int b, double cost) {
            this.querytime = a;
            this.resultsize = b;
            this.querycost = cost;
        }
    }

    abstract protected String setUpQuery(String sql);
    abstract protected measured_result getQueryProcessingTime(String sql);
//    abstract protected String compileResultRow(long sql1, long sql2, String index, int B_count, int result_size, bench_config bconfig, String query);
    abstract protected String compileResultRow(measured_result sql1, measured_result sql2, String index, int B_count, bench_config bconfig, String query);
    abstract protected String compileResultRowHeader();

    public void run() throws Exception {
        // create querytime list of table names "R100", "R300", "R1000", "R3000" ...
        tableNames = generateTableNames(bconfig.tab_prefix);

        readCreateCommands(createindexes, createindexes_legends, bconfig.CREATEINDEXES_FILENAME);
        readDropCommands(dropindexes,  bconfig.DROPINDEXES_FILENAME);
        if (createindexes.size() != dropindexes.size()) {
            throw new Exception("Number of create and drop indexes commands must be equal");
        }

        for (Pair<String, String> pair : bconfig.queryFileNames) {
            queries.add(pair);
        }

        System.out.println(compileResultRowHeader());
        bconfig.logger.info(compileResultRowHeader());

        // run initial drop index commands
        for (int i = 0; i < dropindexes.size(); i++) {
            String dropindex = dropindexes.get(i);
            runCommand(dropindex, tableNames);
        }

        for (int i = 0; i < createindexes.size(); i++) {
            String createindex = createindexes.get(i);
            String dropindex = dropindexes.get(i);

            runCommand(createindex, tableNames);

            for (int j = 0; j < queries.size(); j++) {
                Pair<String,String> sql = queries.get(j);
                String index_legend = createindexes_legends.get(i);
                benchmarkSQLrewrite(sql, index_legend, tableNames);
            }

            runCommand(dropindex, tableNames);
        }
    }


    protected void runCommand(String sql_index, ArrayList<Pair<String, Integer>> tableNames) {
        System.out.println("----------------------------------------");
        System.out.println("DDL: " + sql_index);

        if (sql_index.isEmpty()) return;
        bconfig.logger.info("DDL: " + sql_index);
        for (Pair<String, Integer> tableName : tableNames) {
            String cmd = sql_index.replace("tab", tableName.a);

            if (bconfig.USERNAME.trim().equals(""))
            {
                try (Connection connection = DriverManager.getConnection(bconfig.CONNECTION_STRING);
                     Statement statement = connection.createStatement()) {
                    statement.execute(cmd);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            } else {
                try (Connection connection = DriverManager.getConnection(bconfig.CONNECTION_STRING, bconfig.USERNAME, bconfig.PASSWORD);
                     Statement statement = connection.createStatement()) {
                    statement.execute(cmd);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private boolean benchmarkSQLrewrite(Pair<String,String> sqlInit, String index_legend, ArrayList<Pair<String, Integer>> tableNames) {

//        System.out.println("Groups,SQL1,SQL2");
        for (Pair<String, Integer> tableName : tableNames) {
            String sql1 = sqlInit.a.replace("TAB", "\"" + tableName.a + "\"");
            String sql2 = rewriteSQL(sql1);
            sql1 = setUpQuery(sql1);
            sql2 = setUpQuery(sql2);
            bconfig.logger.info("SQL1:\n" + sql1);
            bconfig.logger.info("SQL2:\n" + sql2);

            // execute the queries three times and get the lowest time
            measured_result sql1_time = processQueryNTimes(sql1, 3);
            measured_result sql2_time = processQueryNTimes(sql2, 3 );

            // check if the result sizes are the same
            if (sql1_time.resultsize != sql2_time.resultsize &&
                    sql2_time.resultsize != -1 &&
                    sql2_time.resultsize != -1) {
                System.out.println("ERROR: result size mismatch. SQL1: " + sql1_time.resultsize + ", SQL2: " + sql2_time.resultsize);
                bconfig.logger.info("ERROR: result size mismatch!");
                return false;
            }

            System.out.println(compileResultRow(sql1_time, sql2_time, index_legend, tableName.b, bconfig, sqlInit.b));
            bconfig.logger.info(compileResultRow(sql1_time, sql2_time, index_legend, tableName.b, bconfig, sqlInit.b));
        }

        return true;
    }

    /**
     * Execute the sql query n times and return the lowest time and result size
     * @param sql sql query
     * @param n number of times to execute the query
     * @return measured_result
     */
    private measured_result processQueryNTimes(String sql, int n) {
        long lowest_time;
        measured_result return_pair = null;
        ArrayList<measured_result> sql_times = new ArrayList<measured_result>();
        for (int i = 0; i < n; i++) {
            measured_result result = getQueryProcessingTime(sql);
            sql_times.add(result);
            if (result.querytime >= (long)60000) {
                // if the query took more than 1 minute, return the result
                return result;
            }
        }
        // find lowest value in sql_times
        lowest_time = Long.MAX_VALUE;
        for (int i = 0; i < n; i++) {
            if (sql_times.get(i).querytime < lowest_time) {
                lowest_time = sql_times.get(i).querytime;
                return_pair = sql_times.get(i);
            }
        }
        return return_pair;
    }

    private String rewriteSQL(String sql) {
        ANTLRInputStream input = new ANTLRInputStream(sql.toUpperCase());
        Mssql_lexer lexer = new Mssql_lexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        Mssql parser = new Mssql(tokens);

        ParseTree tree = parser.tsql_file(); // begin parsing at init rule
        Mssql_rewriter_visitor visitor = new Mssql_rewriter_visitor();
        visitor.setConfig(bconfig.config);
        visitor.visit(tree);
        return visitor.getSelectCmd().getQueryText();

    }

    public void readDropCommands(ArrayList<String> dropindexes, String indexFileName) {
        try (FileReader fileReader = new FileReader(indexFileName);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                dropindexes.add(line);
            }
        } catch (IOException e) {
            System.out.println("Error reading file with DDL commands: " + e.getMessage());
        }
    }

    public void readCreateCommands(ArrayList<String> createindexes, ArrayList<String> indexlegends, String indexFileName) {
        try (FileReader fileReader = new FileReader(indexFileName);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] parts = line.split(":");
                if (parts.length < 2) {
                    indexlegends.add(" ");
                    createindexes.add(" ");
                }  else {
                    indexlegends.add(parts[0]);
                    createindexes.add(parts[1]);
                }
            }
        } catch (IOException e) {
            System.out.println("Error reading file with DDL commands: " + e.getMessage());
        }
    }

//    public Pair<String, String> readQueryFromFile(String fileName) {
//        String sqlInit = "";
//        String sqlLegend = "";
//        boolean firstLine = true;
//        try (FileReader fileReader = new FileReader(fileName);
//             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
//            String line;
//            while ((line = bufferedReader.readLine()) != null) {
//                if (firstLine) {
//                    firstLine = false;
//                    sqlLegend = line;
//                    continue;
//                }
//                sqlInit += "\n" + line;
//            }
//        } catch (IOException e) {
//            System.out.println("Error reading file: " + e.getMessage());
//        }
//        return new Pair(sqlInit, sqlLegend);
//    }


    public static ArrayList<Pair<String, String>> generateQueriesWithSelectivity(ArrayList<Integer> selectivity, String fileName) {
        ArrayList<Pair<String, String>> queries = new ArrayList<Pair<String, String>>();
        for (Integer sel : selectivity) {
            Pair<String, String> temp = benchmark.readQueryFromFile(fileName);
            String temp_a = temp.a.replace("SSS", sel.toString());
            String temp_b = temp.b.replace("SSS", sel.toString());
            queries.add(new Pair<String, String>(temp_a, temp_b));
        }
        return queries;
    }

    public static Pair<String, String> readQueryFromFile(String fileName) {
        String sqlInit = "";
        String sqlLegend = "";
        boolean firstLine = true;
        try (FileReader fileReader = new FileReader(fileName);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (firstLine) {
                    firstLine = false;
                    sqlLegend = line;
                    continue;
                }
                sqlInit += "\n" + line;
            }
        } catch (IOException e) {
            System.out.println("Error reading file: " + e.getMessage());
        }
        return new Pair(sqlInit, sqlLegend);
    }

    public ArrayList<Pair<String, Integer>> generateTableNames(String tab_prefix) {
        ArrayList<Pair<String, Integer>> tableNames = new ArrayList<Pair<String, Integer>>();
        for (int i = 1; i <= 4; i++) {
            // convert pow result into integer
            tableNames.add(new Pair(tab_prefix + (int) pow(10, i), (int) pow(10, i)));
            tableNames.add(new Pair(tab_prefix + 3 * (int) pow(10, i), 3 * (int) pow(10, i)));
        }
        return tableNames;
    }
}
