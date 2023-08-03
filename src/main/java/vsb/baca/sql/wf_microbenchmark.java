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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class wf_microbenchmark {
    public enum modes { microbenchmark, rewrite};

    public static void main(String[] args) throws Exception {
        // Parse input parameters
        String dbSystem = null;
        String hostname = null;
        String username = null;
        String password = null;
        String testType = null;
        String logicalTree = null;
        String fileName = null;

        // Define the possible values for database system and test type
        List<String> possibleDbSystems = Arrays.asList("oracle", "postgresql", "mssql", "mysql");
        List<String> possibleTestTypes = Arrays.asList("agg", "rank_alg", "rank", "probe", "joinnmin", "unit_test");
        List<String> possibleLogicalTrees = Arrays.asList("lateralagg", "laterallimitties", "lateraldistinctlimitties", "joinmin", "bestfit");

        if (args.length == 0) {
            printHelp();
            return;
        }

        modes option = null;
        String str_option = args[0];

        if (str_option.equals("-m")) {
            option = modes.microbenchmark;
        }
        else if (str_option.equals("-r")) {
            option = modes.rewrite;
        }
        else {
            System.out.println("Invalid option: " + str_option);
            printHelp();
            return;
        }

        // Check the number of input parameters
        if ((option == modes.microbenchmark && args.length != 11) ||
                (option == modes.rewrite && args.length != 7)) {
            System.out.println("Invalid number of parameters");
            printHelp();
            return;
        }

        // Process the input parameters
        for (int i = 1; i < args.length; i += 2) {
            String key = args[i];
            String value = args[i + 1];

            switch (key) {
                case "-d":
                    value = value.toLowerCase();
                    if (!possibleDbSystems.contains(value)) {
                        System.out.println("Invalid database system value. Possible values: " + possibleDbSystems);
                        return;
                    }
                    dbSystem = value;
                    break;
                case "-h":
                    hostname = value;
                    break;
                case "-u":
                    username = value;
                    break;
                case "-p":
                    password = value;
                    break;
                case "-t":
                    value = value.toLowerCase();
                    if (!possibleTestTypes.contains(value)) {
                        System.out.println("Invalid test type value. Possible values: " + possibleTestTypes);
                        return;
                    }
                    testType = value;
                    break;
                case "-l":
                    value = value.toLowerCase();
                    if (!possibleLogicalTrees.contains(value)) {
                        System.out.println("Invalid logical_tree value. Possible values: LateralAgg, LateralLimitTies, LateralDistinctLimitTies, JoinMin");
                        return;
                    }
                    logicalTree = value;
                    break;
                case "-f":
                    fileName = value;
                    break;
                default:
                    System.out.println("Invalid parameter: " + key);
                    printHelp();
                    return;
            }
        }

        // Check if all required parameters are provided
        if (option == modes.microbenchmark && (dbSystem == null || hostname == null || username == null || password == null || testType == null)) {
            System.out.println("Required parameters are missing");
            printHelp();
            return;
        }
        if (option == modes.rewrite && (dbSystem == null || logicalTree == null || fileName == null)) {
            System.out.println("Required parameters are missing");
            printHelp();
            return;
        }

        // perform the main logic
        if (option == modes.microbenchmark) {
            run_microbenchmark(dbSystem, hostname, username, password, testType);
        }
        else if (option == modes.rewrite) {
            run_rewrite(dbSystem, logicalTree, fileName);
        }
    }

    private static void run_rewrite(String dbSystem, String logicalTree, String fileName) {
        String inputSql;
        try {
            inputSql = readInputFile(fileName).toUpperCase(Locale.ROOT);
        } catch (IOException e) {
            System.out.println("Error reading input file: " + e.getMessage());
            return;
        }

        String outputSql = rewriteSQL(inputSql, dbSystem, logicalTree);
        System.out.println(outputSql);
    }

    private static void run_microbenchmark(String dbSystem, String hostname, String username, String password, String testType) throws Exception {
        if (dbSystem.contains("oracle")) {
            String connection_string = "jdbc:oracle:thin:@" + hostname;
            System.out.println("Connection string: " + connection_string);
            if (testType.contains("agg"))
                oracle_agg_test.run(connection_string, username, password);
            else if (testType.contains("rank_alg"))
                oracle_rank_algorithm_test.run(connection_string, username, password);
            else if (testType.contains("rank"))
                oracle_rank_test.run(connection_string, username, password);
            else if (testType.contains("probe"))
                oracle_rewrite_probe.run(connection_string, username, password);
        }

        if (dbSystem.contains("mssql")) {
            String connection_string = "jdbc:sqlserver://" + hostname + ";user=" + username + ";password=" + password + ";";
            System.out.println("Connection string: " + connection_string);
            if (testType.contains("agg"))
                mssql_agg_test.run(connection_string, "", "", "row");
            else if (testType.contains("rank_alg"))
                mssql_rank_algorithm_test.run(connection_string, "", "", "row");
            else if (testType.contains("rank"))
                mssql_rank_test.run(connection_string, "", "", "row");
            else if (testType.contains("probe"))
                mssql_rewrite_probe.run(connection_string);
            else if (testType.contains("unit_test"))
                mssql_unit_test.run(connection_string, "", "");
        }

        if (dbSystem.contains("postgresql")) {
            String connection_string = "jdbc:postgresql://" + hostname;
            System.out.println("Connection string: " + connection_string);
            if (testType.contains("agg"))
                postgre_agg_test.run(connection_string, username, password);
            else if (testType.contains("rank_alg"))
                postgre_rank_algorithm_test.run(connection_string, username, password);
            else if (testType.contains("rank"))
                postgre_rank_test.run(connection_string, username, password);
            else if (testType.contains("probe"))
                postgre_rewrite_probe.run(connection_string, username, password);
            else if (testType.contains("joinnmin"))
                postgre_rank_NMIN_algorithm_test.run(connection_string, username, password);
            else if (testType.contains("unit_test"))
                postgre_unit_test.run(connection_string, username, password);
        }

        if (dbSystem.contains("mysql")) {
            String connection_string = "jdbc:mysql://" + hostname;
            System.out.println("Connection string: " + connection_string);
            if (testType.contains("agg"))
                mysql_agg_test.run(connection_string, username, password);
            else if (testType.contains("rank"))
                mysql_rank_test.run(connection_string, username, password);
        }
    }

    private static String readInputFile(String fileName) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
        }
        return sb.toString();
    }

    private static String rewriteSQL(String sql, String dbSystem, String logicalTree) {
        sql = sql.toUpperCase(Locale.ROOT);
        ANTLRInputStream input = new ANTLRInputStream(sql);
        Mssql_lexer lexer = new Mssql_lexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        Mssql parser = new Mssql(tokens);

        ParseTree tree = parser.tsql_file(); // begin parsing at init rule
        Mssql_rewriter_visitor visitor = new Mssql_rewriter_visitor();
        Config.dbms selectedDbms = Config.getSelectedDbms(dbSystem);;
        Config.rank_algorithm selectedRankAlgorithm = Config.getRank_algorithm(logicalTree);

        visitor.setConfig(new Config(selectedDbms, false, selectedRankAlgorithm));
        visitor.visit(tree);
        return visitor.getSelectCmd().getQueryText();

    }

    private static void printHelp() {
        System.out.println("Usage: java -jar sql.jar -m -d <database_system> -h <hostname> -u <username> -p <password> -t <test_type>");
        System.out.println("       (to run microbenchmark)");
        System.out.println("   or  java -jar sql.jar -r -d <database_system> -l <logical_tree> -f <file_name>");
        System.out.println("       (to rewrite SQL)");
        System.out.println("Possible values:");
        System.out.println("  -d: Oracle, PostgreSql, MSSql");
        System.out.println("  -h: Hostname including port and database (SID) name if necessary. Use predefined connection string format of that database system. Start with host name.");
        System.out.println("  -t: Agg, Rank_alg, Rank, Probe, JoinNMin");
        System.out.println("  -l: LateralAgg, LateralLimitTies, LateralDistinctLimitTies, JoinMin, BestFit");
    }
}