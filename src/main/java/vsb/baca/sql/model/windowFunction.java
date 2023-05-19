package vsb.baca.sql.model;


import org.antlr.v4.runtime.misc.Pair;
import vsb.baca.grammar.rewriter.sqlUtil;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.ArrayList;

public class windowFunction {
    private String                  functionName;
    private String                  winFunAlias;
    private String                  range_row_frameBounds; // possible values are RANGE, ROWS, NONE
    private String                  frameStart;
    private String                  frameEnd;
    private ParserRuleContext       functionExpression;
    private ParserRuleContext       functionContext; // expression element rooting whole window function
    private ArrayList<ParserRuleContext> partitionByList = new ArrayList<ParserRuleContext>();
    private ArrayList<Pair<ParserRuleContext,Boolean>> orderByList = new ArrayList<Pair<ParserRuleContext,Boolean>>();
    private ArrayList<predicate>    predicateList = new ArrayList<predicate>();
    private String                  remainder_equality_condition;
    private Config                  config;

    interface OrderByCondition {
        boolean check(int index, String functionName);
    }
    OrderByCondition simpleOrdering = (index, functionName) -> {
        return !orderByList.get(index).b;
    };
    OrderByCondition reverseOrdering = (index, functionName) -> {
        return orderByList.get(index).b;
    };
    OrderByCondition lagLeadOrdering = (index, functionName) -> {
        return (!orderByList.get(index).b && (functionName.equals("LAG")) ||
                (orderByList.get(index).b && functionName.equals("LEAD")));
    };

    public windowFunction(Config config) {
    }

    public windowFunction(String functionName,
                            String winFunAlias,
                          ParserRuleContext functionExpression,
                          ParserRuleContext functionContext,
                          Config config) {
        this.functionName = functionName;
        this.winFunAlias = winFunAlias;
        this.functionExpression = functionExpression;
        this.functionContext = functionContext;
        this.config = config;
    }

    public String getRemainderEqualityCondition() {
        return remainder_equality_condition;
    }

    public void resetRemainderEqualityCondition() {
        remainder_equality_condition = null;
    }

    public void setRange_row_frameBounds(String range_row_frameBounds) {
        this.range_row_frameBounds = range_row_frameBounds;
    }

    public void setFrameStart(String frameStart) {
        this.frameStart = frameStart;
    }

    public void setFrameEnd(String frameEnd) {
        this.frameEnd = frameEnd;
    }

    public ArrayList<ParserRuleContext> getPartitionByList() {
        return partitionByList;
    }

    public ArrayList<Pair<ParserRuleContext,Boolean>> getOrderByList() {
        return orderByList;
    }

    public void addOrderByExpression(ParserRuleContext orderByExpression, boolean isDescending) {
        orderByList.add(new Pair<ParserRuleContext,Boolean>(orderByExpression, isDescending));
    }

    public String getWinFunAlias() {
        return winFunAlias;
    }

    public String getFunctionName() {
        return functionName;
    }

    public ParserRuleContext getFunctionContext() {
        return functionContext;
    }

    public void addPredicate(predicate pred) {
        predicateList.add(pred);
    }

    public boolean isJoinRewrite() {
        if ((functionName.equalsIgnoreCase("RANK") ||
                functionName.equalsIgnoreCase("DENSE_RANK")) &&
                predicateList.size() == 1 &&
                !config.getAttributesCanBeNull()) {
            predicate p = predicateList.get(0);
            if (p.comparisonOp == predicate.comparisonOperator.EQUAL && p.right == 1)
                return true;
        }
        return false;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////// Query building methods /////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    public String getQueryText(String subqueryString, String alias)
    {
        StringBuilder builder = new StringBuilder();
        StringBuilder builder_sub = new StringBuilder();

        remainder_equality_condition = null;
        if (functionName.equalsIgnoreCase("DENSE_RANK") ||
            functionName.equalsIgnoreCase("RANK") ||
            functionName.equalsIgnoreCase("ROW_NUMBER"))
        {
            // there is no predicate allowing rewriting in the outer query
            // window function should be preserved in the main subquery
            // however in such case the rewrite may loose the meaning
            if (predicateList.size() == 0) throw new RuntimeException("There is no predicate allowing rewriting of "+ functionName);
            if (predicateList.size() > 1)  throw new RuntimeException("There is more than one predicate in outer query referencing the alias of window function. Unfortunately such rewrite is not supported");

            predicate p = predicateList.get(0); // we assume that there is only one predicate
            if (p.comparisonOp == predicate.comparisonOperator.EQUAL) {
                if (p.right != 1) {
                    throw new RuntimeException("DENSE_RANK/RANK/ROW_NUMBER rewrite does not support EQUAL predicate with value other than 1 in MSSQL");
                }
            }
            if (p.comparisonOp == predicate.comparisonOperator.LESS_THAN || p.comparisonOp == predicate.comparisonOperator.LESS_THAN_OR_EQUAL) {
                if (functionName.equals("DENSE_RANK")) {
                    throw new RuntimeException("DENSE_RANK rewrite does not support LESS THAN predicate");
                }
            }
            assert (orderByList.size() > 0);
            wfRank(subqueryString, alias, builder);
        }

        if (functionName.equalsIgnoreCase("LAG") ||
                functionName.equalsIgnoreCase("LEAD")) {
            wfLagLead(subqueryString, builder);
        }

        if (functionName.equalsIgnoreCase("MAX") ||
                functionName.equalsIgnoreCase("AVG") ||
                functionName.equalsIgnoreCase("MIN") ||
                functionName.equalsIgnoreCase("SUM") ||
                functionName.equalsIgnoreCase("COUNT")) {
            if (range_row_frameBounds.equals("RANGE")) {
                wfRangeAggregate(subqueryString, builder);
            }
            if (range_row_frameBounds.equals("ROWS")) {
                wfRowsAggregate(subqueryString, builder, builder_sub);
            }
        }
        return builder.toString();
    }

    private void wfRank(String subqueryString, String alias, StringBuilder builder)
    {
        if (isJoinRewrite()) {
            if (orderByList.size() > 1) {
                builder.append(" SELECT * FROM ( SELECT ");
                buildSimpleListFromPartitionBy(builder, false);
                // TODO: finish a situation when there are more than one order by attributes
            } else {
                // this is a simple situaton when there is only one order by attribute
                builder.append(" SELECT ");
                buildSimpleListFromPartitionBy(builder, false);
                builder.append(", MIN(");
                String minAttribute = sqlUtil.getText(orderByList.get(0).a).trim();
                builder.append(minAttribute);
                builder.append(") min_" + minAttribute + ", 1 " + winFunAlias);
                builder.append(" FROM (" + subqueryString + ") AS winfun_subquery");
                builder.append(" GROUP BY ");
                buildSimpleListFromPartitionBy(builder, false);
                // remainder builder
                StringBuilder remainder_builder = new StringBuilder();
                buildEqualityWhereCondition(partitionByList, remainder_builder, alias);
                if (partitionByList.size() > 0)
                    remainder_builder.append(" AND ");
                remainder_builder.append(alias + ".min_" + minAttribute + " = main_subquery." + minAttribute);
                remainder_equality_condition = remainder_builder.toString();
            }
        } else {
            if (config.getSelectedDbms() == Config.dbms.MSSQL) {
                builder.append(" SELECT ");
                predicate p = predicateList.get(0); // we assume that there is only one predicate (it is controled in getQueryText method)
                if (p.comparisonOp == predicate.comparisonOperator.EQUAL) {
                    builder.append(" TOP " + p.right + " WITH TIES ");
                }
                if (p.comparisonOp == predicate.comparisonOperator.LESS_THAN || p.comparisonOp == predicate.comparisonOperator.LESS_THAN_OR_EQUAL) {
                    int equal_correction = 1;
                    if (p.comparisonOp == predicate.comparisonOperator.LESS_THAN_OR_EQUAL) {
                        equal_correction = 0;
                    }
                    builder.append(" TOP " + (p.right - equal_correction) + " WITH TIES ");
                }

                builder.append(" 1 " + winFunAlias);
            } else {
                builder.append(" SELECT 1 " + winFunAlias);
            }
            buildSimpleListFromOrderBy(builder, true);
            builder.append(" FROM (" + subqueryString + ") AS winfun_subquery");
            if (partitionByList.size() > 0) {
                builder.append(" WHERE ");
                buildEqualityWhereCondition(partitionByList, builder, null);
            }
            builder.append(" ORDER BY ");
            buildOrderBy(builder, simpleOrdering);
            if (config.getSelectedDbms() != Config.dbms.MSSQL) {
                // MSSQL does not support OFFSET/FETCH WITH TIES, therefore, we use TOP N WITH TIES
                buildOffsetFetch(builder, functionName);
            }

            // remainder builder
            StringBuilder remainder_builder = new StringBuilder();
            ArrayList<ParserRuleContext> list = new ArrayList<ParserRuleContext>();
            for (int i = 0; i < orderByList.size(); i++) {
                list.add(orderByList.get(i).a);
            }
            buildEqualityWhereCondition(list, remainder_builder, alias);
            remainder_equality_condition = remainder_builder.toString();
        }
    }

    private void wfRowsAggregate(String subqueryString, StringBuilder builder, StringBuilder builder_sub) {
        if (functionExpression == null) {
            builder.append(" SELECT " + functionName + "(*) " + winFunAlias + " FROM ( ");
            builder_sub.append(" SELECT * FROM (" + subqueryString + ") AS winfun_subquery");
        } else {
            builder.append(" SELECT " + functionName + "(" + sqlUtil.getText(functionExpression) + ") " + winFunAlias + " FROM ( ");
            builder_sub.append(" SELECT " + sqlUtil.getText(functionExpression) + " FROM (" + subqueryString + ") AS winfun_subquery");
        }
        if (partitionByList.size() > 0 || orderByList.size() > 0) {
            builder_sub.append(" WHERE ");
        }
        if (partitionByList.size() > 0) {
            buildEqualityWhereCondition(partitionByList, builder_sub, null);
        }
        assert (orderByList.size() > 0);
        predicate.comparisonOperator comparisonOp = null;
        int startNumber = sqlUtil.parseFrameBound(frameStart);
        int endNumber = sqlUtil.parseFrameBound(frameEnd);
        if (startNumber < 0 || startNumber == Integer.MIN_VALUE) {
            comparisonOp = predicate.comparisonOperator.LESS_THAN_OR_EQUAL;
            builder.append(builder_sub);
            buildOrderByWhereCondition(builder, comparisonOp);
            builder.append(" ORDER BY ");
            buildOrderBy(builder, reverseOrdering);
            if (endNumber <= 0) {
                builder.append(" OFFSET " + (-endNumber) + " ROWS ");
                if (startNumber != Integer.MIN_VALUE) {
                    builder.append(" FETCH FIRST " + (-startNumber + endNumber + 1) + " ROWS ONLY ");
                }
            } else {
                builder.append(" OFFSET 0 ROWS ");
                if (startNumber != Integer.MIN_VALUE) {
                    builder.append(" FETCH FIRST " + (-startNumber + 1) + " ROWS ONLY ");
                }
            }
        }

        if (endNumber > 0 || endNumber == Integer.MIN_VALUE) {
            if (startNumber >= 0) {
                comparisonOp = predicate.comparisonOperator.GREATER_THAN_OR_EQUAL;
            } else {
                builder.append(" UNION ALL ");
                comparisonOp = predicate.comparisonOperator.GREATER_THAN;
            }
            builder.append(builder_sub);
            buildOrderByWhereCondition(builder, comparisonOp);
            builder.append(" ORDER BY ");
            buildOrderBy(builder, simpleOrdering);
            if (startNumber >= 0) {
                builder.append(" OFFSET " + (startNumber) + " ROWS ");
                if (endNumber != Integer.MIN_VALUE) {
                    builder.append(" FETCH FIRST " + (endNumber - startNumber + 1) + " ROWS ONLY ");
                }
            } else {
                builder.append(" OFFSET 0 ROWS ");
                if (endNumber != Integer.MIN_VALUE) {
                    builder.append(" FETCH FIRST " + endNumber + " ROWS ONLY ");
                }
            }
        }
        builder.append(" ) AS winfun_subquery_rows");

    }

     private void wfRangeAggregate(String subqueryString, StringBuilder builder) {
        if (functionExpression == null) {
            builder.append(" SELECT " + functionName + "(*) " + winFunAlias);
        } else {
            builder.append(" SELECT " + functionName + "(" + sqlUtil.getText(functionExpression) + ") " + winFunAlias);
        }
        builder.append(" FROM (" + subqueryString + ") AS winfun_subquery");
        if (partitionByList.size() > 0 || orderByList.size() > 0) {
            builder.append(" WHERE ");
        }
        if (partitionByList.size() > 0) {
            buildEqualityWhereCondition(partitionByList, builder, null);
        }
        if (orderByList.size() > 0) {

            predicate.comparisonOperator comparisonOp = null;
            if (frameStart.equals("UNBOUNDED") && frameEnd.equals("CURRENT")) {
                comparisonOp = predicate.comparisonOperator.LESS_THAN_OR_EQUAL;
            }
            if (frameStart.equals("CURRENT") && frameEnd.equals("UNBOUNDED")) {
                comparisonOp = predicate.comparisonOperator.GREATER_THAN_OR_EQUAL;
            }
            buildOrderByWhereCondition(builder, comparisonOp);
        }
    }

    private void wfLagLead(String subqueryString, StringBuilder builder) {
        builder.append(" SELECT " + sqlUtil.getText(functionExpression) + " " + winFunAlias + " FROM (" + subqueryString + ") AS winfun_subquery");
        if (partitionByList.size() > 0 || orderByList.size() > 0) {
            builder.append(" WHERE ");
        }
        if (partitionByList.size() > 0) {
            buildEqualityWhereCondition(partitionByList, builder, null);
        }
        assert (orderByList.size() > 0);
        predicate.comparisonOperator comparisonOp;
        if (functionName.equalsIgnoreCase("LAG"))
            comparisonOp = predicate.comparisonOperator.LESS_THAN;
        else
            comparisonOp = predicate.comparisonOperator.GREATER_THAN;
        buildOrderByWhereCondition(builder, comparisonOp);
        builder.append(" ORDER BY ");
        buildOrderBy(builder, lagLeadOrdering);
        builder.append(" OFFSET 0 ROWS ");
        builder.append(" FETCH FIRST 1 ROWS ONLY ");
    }

    /**
     * Build a OFFSET and FETCH for a DENSE_RANK window function
     * @param builder
     */
    private void buildOffsetFetch(StringBuilder builder, String functionName) {
        if (predicateList.size() == 1) {
            predicate p = predicateList.get(0);
            if (p.comparisonOp == predicate.comparisonOperator.EQUAL) {
                builder.append(" OFFSET " + (p.right - 1) + " ROWS ");
                builder.append(" FETCH FIRST 1 ROWS WITH TIES ");
            }
            if (p.comparisonOp == predicate.comparisonOperator.LESS_THAN || p.comparisonOp == predicate.comparisonOperator.LESS_THAN_OR_EQUAL) {
                if (functionName.equals("DENSE_RANK")) {
                    throw new RuntimeException("DENSE_RANK rewrite does not support LESS THAN predicate");
                }
                int offset = 1;
                if (p.comparisonOp == predicate.comparisonOperator.LESS_THAN_OR_EQUAL) {
                    offset = 0;
                }
                builder.append(" OFFSET 0 ROWS ");
                builder.append(" FETCH FIRST " + (p.right - offset) +" ROWS WITH TIES ");
            }

        } else
        {
            throw new RuntimeException("DENSE_RANK and RANK rewrite does not support more than one predicate");
            // TODO ??
            // this should cover situations where there is more than one predicate in the outer query referencing the rank result
        }
    }

    /**
     * Builds the WHERE condition from a list of attributes, that needs to be equal.
     * Typicaly it is used when we need a condition for the PARTITION BY clause
     * @param builder
     */
    private void buildEqualityWhereCondition(ArrayList<ParserRuleContext> list, StringBuilder builder, String alias) {
        if (alias == null) alias = "winfun_subquery";
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
                builder.append(" AND ");
            }
            builder.append("(" + alias + "." + sqlUtil.getText(list.get(i)) + " = ");
            builder.append("main_subquery." + sqlUtil.getText(list.get(i)));
            builder.append(" OR (");
            builder.append(alias + "." + sqlUtil.getText(list.get(i)) + " IS NULL AND ");
            builder.append("main_subquery." + sqlUtil.getText(list.get(i)) + " IS NULL))");
        }
    }

    /**
     * Builds the ORDER BY clause
     * @param builder
     */
    private void buildOrderBy(StringBuilder builder, OrderByCondition condition) {
        for (int i = 0; i < orderByList.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            if (condition.check(i, functionName))
                builder.append(sqlUtil.getText(orderByList.get(i).a) + " ASC ");
            else
                builder.append(sqlUtil.getText(orderByList.get(i).a) + " DESC ");
        }
    }

    /**
     * Builds a list of attributes from the ORDER BY clause
     * @param builder
     */
    private void buildSimpleListFromOrderBy(StringBuilder builder, boolean first_comma) {
        for (int i = 0; i < orderByList.size(); i++) {
            if (first_comma || i > 0) {
                builder.append(", ");
            }
            builder.append(sqlUtil.getText(orderByList.get(i).a));
        }
    }

    /**
     * Builds a list of attributes from the PARTITION BY clause
     * @param builder
     */
    private void buildSimpleListFromPartitionBy(StringBuilder builder, boolean first_comma) {
        for (int i = 0; i < partitionByList.size(); i++) {
            if (first_comma || i > 0) {
                builder.append(", ");
            }
            builder.append(sqlUtil.getText(partitionByList.get(i)));
        }
    }

    /**
     * Builds the WHERE condition for the ORDER BY clause
     * @param builder
     * @param comparisonOp
     */
    private void buildOrderByWhereCondition(StringBuilder builder, predicate.comparisonOperator comparisonOp) {
        if (comparisonOp != null) {
            if (partitionByList.size() > 0) {
                builder.append(" AND (");
            }
            for (int i = 0; i < orderByList.size(); i++) {
                if (i > 0) {
                    builder.append(" OR ");
                }
                builder.append("(");
                for (int j = 0; j < i; j++) {
                    if (j > 0) {
                        builder.append(" AND ");
                    }
                    builder.append("(");
                    builder.append("winfun_subquery." + sqlUtil.getText(orderByList.get(j).a) + " = ");
                    builder.append("main_subquery." + sqlUtil.getText(orderByList.get(j).a));
                    // NULL part
                    builder.append(" OR (");
                    builder.append("winfun_subquery." + sqlUtil.getText(orderByList.get(j).a) + " IS NULL AND ");
                    builder.append("main_subquery." + sqlUtil.getText(orderByList.get(j).a) + " IS NULL");
                    builder.append(")");

                    builder.append(")");
                }
                if (i > 0) {
                    builder.append(" AND ");
                }
                builder.append("(");
                String comparisonOpString = chooseComparisonOp(comparisonOp, i == orderByList.size() - 1, orderByList.get(i).b);
                builder.append("winfun_subquery." + sqlUtil.getText(orderByList.get(i).a) + comparisonOpString);
                builder.append("main_subquery." + sqlUtil.getText(orderByList.get(i).a));
                // NULL part
                builder.append(" OR (");
                if (comparisonOpString.equals("<="))
                    builder.append("winfun_subquery." + sqlUtil.getText(orderByList.get(i).a) + " IS NULL");
                if (comparisonOpString.equals(">="))
                    builder.append("main_subquery." + sqlUtil.getText(orderByList.get(i).a) + " IS NULL");
                if (comparisonOpString.equals("<")) {
                    builder.append("winfun_subquery." + sqlUtil.getText(orderByList.get(i).a) + " IS NULL AND ");
                    builder.append("main_subquery." + sqlUtil.getText(orderByList.get(i).a) + " IS NOT NULL");
                }
                if (comparisonOpString.equals(">")) {
                    builder.append("winfun_subquery." + sqlUtil.getText(orderByList.get(i).a) + " IS NOT NULL AND ");
                    builder.append("main_subquery." + sqlUtil.getText(orderByList.get(i).a) + " IS NULL");
                }
                builder.append(")");

                builder.append(")");
                builder.append(")");
            }

            if (partitionByList.size() > 0) {
                builder.append(")");
            }
        }
    }

    private String chooseComparisonOp(predicate.comparisonOperator comparisonOp, boolean isLast, boolean isDescending) {
        if (isLast) {
            if (comparisonOp == predicate.comparisonOperator.LESS_THAN_OR_EQUAL || comparisonOp == predicate.comparisonOperator.GREATER_THAN_OR_EQUAL) {
                if ((!isDescending && comparisonOp == predicate.comparisonOperator.LESS_THAN_OR_EQUAL) ||
                        (isDescending && comparisonOp == predicate.comparisonOperator.GREATER_THAN_OR_EQUAL)) {
                    return "<=";
                } else {
                    return ">=";
                }
            } else {
                if ((!isDescending && comparisonOp == comparisonOp.LESS_THAN) ||
                        (isDescending && comparisonOp == predicate.comparisonOperator.GREATER_THAN)) {
                    return "<";
                } else {
                    return ">";
                }
            }
        } else {
            if ((!isDescending && (comparisonOp == comparisonOp.LESS_THAN || comparisonOp == predicate.comparisonOperator.LESS_THAN_OR_EQUAL)) ||
                    (isDescending && (comparisonOp == predicate.comparisonOperator.GREATER_THAN || comparisonOp == predicate.comparisonOperator.GREATER_THAN_OR_EQUAL))) {
                return "<";
            } else {
                return ">";
            }
        }
    }
}
