package vsb.baca.sql;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class wf_microbenchmark {
    public static void main(String[] args) throws Exception {
        // Parse input parameters
        String dbSystem = null;
        String hostname = null;
        String username = null;
        String password = null;
        String testType = null;

        // Define the possible values for database system and test type
        List<String> possibleDbSystems = Arrays.asList("oracle", "postgresql", "mssql", "mysql");
        List<String> possibleTestTypes = Arrays.asList("agg", "rank_alg", "rank", "probe", "joinnmin", "unit_test");

        // Check the number of input parameters
        if (args.length != 10) {
            printHelp();
            return;
        }

        // Process the input parameters
        for (int i = 0; i < args.length; i += 2) {
            String key = args[i];
            String value = args[i + 1];

            switch (key) {
                case "-d":
                    value = value.toLowerCase(Locale.ROOT);
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
                    value = value.toLowerCase(Locale.ROOT);
                    if (!possibleTestTypes.contains(value)) {
                        System.out.println("Invalid test type value. Possible values: " + possibleTestTypes);
                        return;
                    }
                    testType = value;
                    break;
                default:
                    System.out.println("Invalid parameter: " + key);
                    printHelp();
                    return;
            }
        }

        // Check if all required parameters are provided
        if (dbSystem == null || hostname == null || username == null || password == null || testType == null) {
            System.out.println("All parameters are required.");
            printHelp();
            return;
        }

        if (dbSystem.contains("oracle"))
        {
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
        if (dbSystem.contains("mssql"))
        {
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
        if (dbSystem.contains("postgresql"))
        {
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
        if (dbSystem.contains("mysql"))
        {
            String connection_string = "jdbc:mysql://" + hostname;
            System.out.println("Connection string: " + connection_string);
            if (testType.contains("agg"))
                mysql_agg_test.run(connection_string, username, password);
            else if (testType.contains("rank"))
                mysql_rank_test.run(connection_string, username, password);
            else if (testType.contains("unit_test"))
                mysql_unit_test.run(connection_string, username, password);
        }
    }

    private static void printHelp() {
        System.out.println("Usage: java Main -d <database_system> -h <hostname> -u <username> -p <password> -t <test_type>");
        System.out.println("Possible values:");
        System.out.println("-d: Oracle, PostgreSql, MSSql");
        System.out.println("-d: Hostname including port and database (SID) name if necessary");
        System.out.println("-t: Agg, Rank_alg");
    }
}
