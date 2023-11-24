package vsb.baca.sql.benchmark;

import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class benchmark_mssql extends benchmark {
    public benchmark_mssql(bench_config_mssql bench_config_mssql) {
        super(bench_config_mssql);
    }

    @Override protected String setUpQuery(String sql) {
        return sql + ((bench_config_mssql)bconfig).OPTION_MAXDOP;
    }

    @Override protected measured_result getQueryProcessingTime(String sql) {
        int queryTimeout = 300;
        try (Connection connection = DriverManager.getConnection(bconfig.CONNECTION_STRING);
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(queryTimeout);
            statement.execute("SET STATISTICS TIME ON");
            ResultSet resultSet = statement.executeQuery(sql);

            int resultSize = 0;
            while (resultSet.next()) {
                resultSize++;
            }
            resultSet.close();

            // read the query processing time
            long longElapsedTime = extractElapsedTimeFromWarning(statement);
            measured_result elapsedTimeFromCatalog = measureUsingSystemCatalog(sql, connection);
            elapsedTimeFromCatalog.resultsize = resultSize;

//            if (longElapsedTime == -1) {
//                throw new RuntimeException("No SQL Server Execution Times found.");
//            } else {
//                return new measured_result(longElapsedTime, resultSize);
//            }
            if (elapsedTimeFromCatalog.querytime == -1) {
                throw new RuntimeException("No SQL Server Execution Times found.");
            } else {
                return elapsedTimeFromCatalog;
            }


        }
        catch (SQLTimeoutException e) {
            return new measured_result((long)queryTimeout * 1000, -1);
        }
        catch (SQLException e) {
            if (e.getMessage().equals("The query has timed out.")) {
                return new measured_result((long)queryTimeout * 1000, -1);
            } else {
                e.printStackTrace();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return new measured_result((long)-1, -1);
    }

    private long extractElapsedTimeFromWarning(Statement statement) throws SQLException {
        long longElapsedTime = -1;
        SQLWarning warning = statement.getWarnings();
        while (warning != null) {
            String[] lines = warning.getMessage().split("\n"); // split warning.getMessage() according \n
            if (lines[1].trim().equals("SQL Server Execution Times:")) {
                Pattern pattern = Pattern.compile("\\d+");
                Matcher matcher = pattern.matcher(lines[2].trim());

                if (matcher.find()) {
                    String cpuTime = matcher.group();
                    if (matcher.find()) {
                        String elapsedTime = matcher.group();
                        longElapsedTime = Long.parseLong(elapsedTime);
                        break;
                    }
                }
            }
            warning = warning.getNextWarning();
        }
        return longElapsedTime;
    }

    @Override protected String compileResultRow(measured_result sql1, measured_result sql2, String index, int B_count, bench_config bconfig, String query)
    {
        return sql1.querytime + "," + sql2.querytime + "," + sql1.querycost + "," + sql2.querycost + "," + B_count + "," + sql1.resultsize + "," +
                bconfig.storage.toString() + "," + index + ",padding_" + bconfig.padding.toString() +
                ",parallel_" + bconfig.parallelism.toString() + "," + bconfig.config.getSelectedRankAlgorithm().toString() +
                "," + query;
    }

    @Override protected String compileResultRowHeader() {
        return "sql_window_query_time,sql_selfjoin_query_time,sql_window_query_cost,sql_selfjoin_query_cost,B_count,result_size,storage,index,padding,parallel,rank_algorithm,query";
    }

    private measured_result measureUsingSystemCatalog(String sql, Connection connection) throws SQLException, Exception {
        // Get the SQL handle for the executed query
        String sqlHandleQuery = "SELECT plan_handle FROM sys.dm_exec_query_stats CROSS APPLY sys.dm_exec_sql_text(sql_handle) WHERE text LIKE ?";
        PreparedStatement handleStatement = connection.prepareStatement(sqlHandleQuery);
        handleStatement.setString(1, sql);
        ResultSet handleResultSet = handleStatement.executeQuery();

        if (handleResultSet.next()) {
            byte[] planHandle = handleResultSet.getBytes("plan_handle");

            // Get the execution time information from sys.dm_exec_query_stats
            String timeQuery = "SELECT last_elapsed_time FROM sys.dm_exec_query_stats WHERE plan_handle = ?";
            PreparedStatement timeStatement = connection.prepareStatement(timeQuery);
            timeStatement.setBytes(1, planHandle);
            ResultSet timeResultSet = timeStatement.executeQuery();

//            if (timeResultSet.next()) {
//                return (long)timeResultSet.getLong("last_elapsed_time");
//            }
//
//            timeResultSet.close();
            String planQuery = "SELECT query_plan FROM sys.dm_exec_query_plan(?)";
            PreparedStatement planStatement = connection.prepareStatement(planQuery);
            planStatement.setBytes(1, planHandle);
            ResultSet planResultSet = planStatement.executeQuery();

            if (timeResultSet.next() && planResultSet.next()) {
                // Access the XML representation of the query plan
                String queryPlanXml = planResultSet.getString("query_plan");
                double statementSubTreeCost = getStatementSubTreeCost(queryPlanXml);

//                // Example: print the XML representation of the query plan
//                System.out.println("Query Plan XML:\n" + queryPlanXml);
//                System.out.println("Query Plan cost:\n" + statementSubTreeCost);

//                return (long) timeResultSet.getLong("last_elapsed_time");
                return new measured_result((long)timeResultSet.getLong("last_elapsed_time"), -1, statementSubTreeCost);
            }

            timeResultSet.close();
            planResultSet.close();
        }
        handleResultSet.close();
        return new measured_result(-1, -1);
    }

    private double getStatementSubTreeCost(String queryPlanXml) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(new java.io.ByteArrayInputStream(queryPlanXml.getBytes()));

        XPathFactory xPathfactory = XPathFactory.newInstance();
        XPath xpath = xPathfactory.newXPath();

        // Define XPath expression to extract the StatementSubTreeCost
        XPathExpression expr = xpath.compile("/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/@StatementSubTreeCost");
        Node statementSubTreeCostNode = (Node) expr.evaluate(document, XPathConstants.NODE);
        double statementSubTreeCost = Double.parseDouble(statementSubTreeCostNode.getNodeValue());

        return statementSubTreeCost;
    }
}
