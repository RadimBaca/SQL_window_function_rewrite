package vsb.baca.sql;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.*;
import vsb.baca.grammar.*;
import vsb.baca.grammar.rewriter.*;
import vsb.baca.sql.model.selectCmd;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class main {
    public static void main(String[] args) throws Exception {
        // read SQL from input file
        String fileName = "sql/input.sql"; // change to the path and name of your input file
        String sql = "";
        try (FileReader fileReader = new FileReader(fileName);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                sql += "\n" + line;
            }
        } catch (IOException e) {
            System.out.println("Error reading file: " + e.getMessage());
        }

        String result = rewriteSQL(sql);

        System.out.println(result);
    }

    private static String rewriteSQL(String sql) {
        ANTLRInputStream input = new ANTLRInputStream(sql);
        Mssql_lexer lexer = new Mssql_lexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        Mssql parser = new Mssql(tokens);

//        try {
            ParseTree tree = parser.tsql_file(); // begin parsing at init rule
            Mssql_rewriter_visitor visitor = new Mssql_rewriter_visitor();
            visitor.visit(tree);
            return visitor.getSelectCmd().getQueryText();

//        } catch (Exception e) {
//            System.out.println("Error parsing input SQL: " + e.getMessage());
//            return "";
//        }
    }


}