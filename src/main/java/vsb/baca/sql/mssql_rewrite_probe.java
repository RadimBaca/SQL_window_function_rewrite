package vsb.baca.sql;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import vsb.baca.grammar.Mssql;
import vsb.baca.grammar.Mssql_lexer;
import vsb.baca.grammar.rewriter.Mssql_rewriter_visitor;
import vsb.baca.sql.model.Config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Locale;

public class mssql_rewrite_probe {

    private static String sql1 = "";
    private static String sql2 = "";

    public static void main(String[] args) throws Exception {
        // read SQL from input file
        String fileName;

        fileName = "sql/unittests/input_test.sql"; // change to the path and name of your input file
        printQueries(testRewriteSqlInFile(fileName), fileName);
    }

    private static void printQueries(boolean ok, String fileName) {
        System.out.println("-----------------------------------------");
        System.out.println(sql1);
        System.out.println("-----------------------------------------");
        System.out.println(sql2);
        System.out.println("-----------------------------------------");
        if (ok) {
            System.out.println("Test passed - " + fileName);
        } else {
            System.out.println("Test failed - " + fileName);
        }
        sql1 = "";
        sql2 = "";
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

        try {
            // Load the SQL Server JDBC driver
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

            // Connect to the database
            Connection conn = DriverManager.getConnection(
                    "jdbc:sqlserver://bayer.cs.vsb.cz;instanceName=sqldb;databaseName=sqlbench_window", "sqlbench", "n3cUmubsbo");

            // execute rewritten SQL
            Statement stmt1 = conn.createStatement();
            ResultSet rs1 = stmt1.executeQuery(sql1);
            ResultSetMetaData rsmd1 = rs1.getMetaData();
            int columnCount1 = rsmd1.getColumnCount();

            // Execute the SQL SELECT command
            Statement stmt2 = conn.createStatement();
            ResultSet rs2 = stmt2.executeQuery(sql2);
            ResultSetMetaData rsmd = rs2.getMetaData();
            int columnCount2 = rsmd.getColumnCount();

            if (columnCount1 != columnCount2) {
                System.out.println("Number of columns in the result set is different - " + fileName);
                return false;
            }

            // Process the result set
            while (rs2.next()) {
                if (!rs1.next()) {
                    System.out.println("Result set count is different - " + fileName);
                    return false;
                }
                for (int i = 1; i <= columnCount1; i++) {
                    if (rs1.getInt(i) != rs2.getInt(i)) {
                        System.out.println("Result set is different - " + fileName);
                        return false;
                    }
                }
            }

            // Close the resources
            rs2.close();
            stmt2.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private static String rewriteSQL(String sql) {
        sql = sql.toUpperCase(Locale.ROOT);
        ANTLRInputStream input = new ANTLRInputStream(sql);
        Mssql_lexer lexer = new Mssql_lexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        Mssql parser = new Mssql(tokens);

        ParseTree tree = parser.tsql_file(); // begin parsing at init rule
        Mssql_rewriter_visitor visitor = new Mssql_rewriter_visitor();
        visitor.setConfig(new Config(Config.dbms.MSSQL, false, true));
        visitor.visit(tree);
        return visitor.getSelectCmd().getQueryText();

    }


}