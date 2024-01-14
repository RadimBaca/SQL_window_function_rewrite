package vsb.baca.sql;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.tree.ParseTree;
import vsb.baca.grammar.Mssql;
import vsb.baca.grammar.Mssql_lexer;
import vsb.baca.grammar.rewriter.Mssql_rewriter_visitor;
import vsb.baca.sql.benchmark.bench_config_hyper;
import vsb.baca.sql.model.Config;

import com.tableau.hyperapi.Connection;
import com.tableau.hyperapi.HyperProcess;
import com.tableau.hyperapi.Result;
import com.tableau.hyperapi.Telemetry;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Locale;

public class hyper_rewrite_probe {

    private static String sql1 = "";
    private static String sql2 = "";

    private static String connection_string;

    public static void run(String connection_string) throws Exception {

        hyper_rewrite_probe.connection_string = connection_string;

        // read SQL from input file
        String fileName;

        fileName = "sql/unittests/input_rewrite_probe.sql"; // change to the path and name of your input file
        printQueries(testRewriteSqlInFile(fileName), fileName);
    }

    private static void printQueries(boolean ok, String fileName) {
        System.out.println("-----------------------------------------");
        System.out.println(sql1);
        System.out.println("-----------------------------------------");
        System.out.println(sql2);
        System.out.println("-----------------------------------------");
    }

    private static boolean testRewriteSqlInFile(String fileName) {

        try (FileReader fileReader = new FileReader(fileName);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                sql1 += "\n" + line;
            }
        } catch (IOException e) {
            System.out.println("Error reading file: " + e.getMessage());
        }
        sql1 = sql1.toUpperCase();
        sql2 = rewriteSQL(sql1);

//            // execute rewritten SQL
//            Statement stmt1 = conn.createStatement();
//            ResultSet rs1 = stmt1.executeQuery(sql1);
//            ResultSetMetaData rsmd1 = rs1.getMetaData();
//            int columnCount1 = rsmd1.getColumnCount();

        Path pathToDatabase = resolveExampleFile(connection_string);

        if (process_query(pathToDatabase, sql1)) return false;
        if (process_query(pathToDatabase, sql1)) return false;
        if (process_query(pathToDatabase, sql1)) return false;

        System.out.println("--------------------------------------------------");
        if (process_query(pathToDatabase, sql2)) return false;
        if (process_query(pathToDatabase, sql2)) return false;
        if (process_query(pathToDatabase, sql2)) return false;

        return true;
    }

    private static boolean process_query(Path pathToDatabase, String sql) {
        try (HyperProcess process = new HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU);
             com.tableau.hyperapi.Connection connection = new com.tableau.hyperapi.Connection(
                     process.getEndpoint(),
                     pathToDatabase.toString())) {

            long startTime = System.currentTimeMillis();
            Result result = connection.executeQuery(sql);
//            Result result = connection.executeQuery("EXPLAIN (VERBOSE, OPTIMIZERSTEPS) " + sql);

            // read all rows from the result set
            int counter = 0;
            while (result.nextRow()) {
                counter++;
                //System.out.println(result.getString(0));
            }
            System.out.println("Rows: " + counter);

            long endTime = System.currentTimeMillis();
            System.out.println("Time: " + (endTime - startTime) + " ms");
        }
        catch (Exception e) {
            e.printStackTrace();
            return true;
        }
        return false;
    }

    private static String rewriteSQL(String sql) {
        sql = sql.toUpperCase(Locale.ROOT);
        ANTLRInputStream input = new ANTLRInputStream(sql);
        Mssql_lexer lexer = new Mssql_lexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        Mssql parser = new Mssql(tokens);

        ParseTree tree = parser.tsql_file(); // begin parsing at init rule
        Mssql_rewriter_visitor visitor = new Mssql_rewriter_visitor();
        visitor.setConfig(new Config(Config.dbms.POSTGRESQL, false, Config.rank_algorithm.LateralDistinctLimit));
        visitor.visit(tree);
        return visitor.getSelectCmd().getQueryText();

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