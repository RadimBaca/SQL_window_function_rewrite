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
    private selectCmd               root;

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

    public windowFunction(String functionName,
                            String winFunAlias,
                          ParserRuleContext functionExpression,
                          ParserRuleContext functionContext,
                          Config config,
                          selectCmd root) {
        this.functionName = functionName;
        this.winFunAlias = winFunAlias;
        this.functionExpression = functionExpression;
        this.functionContext = functionContext;
        this.config = config;
        this.root = root;
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
                functionName.equalsIgnoreCase("DENSE_RANK")||
                functionName.equalsIgnoreCase("ROW_NUMBER")) &&
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
            if (predicateList.size() > 1)  throw new RuntimeException("There is more than one predicate in outer query referencing the alias of window function. Unfortunately such rewrite is not supported yet.");
            predicate p = predicateList.get(0); // we assume that there is only one predicate
            if (p.comparisonOp != predicate.comparisonOperator.EQUAL &&
                    p.comparisonOp != predicate.comparisonOperator.LESS_THAN_OR_EQUAL &&
                    p.comparisonOp != predicate.comparisonOperator.LESS_THAN) {
                throw new RuntimeException("There is no suitable predicate allowing rewriting of "+ functionName +
                        ". This implementation supports just operators '<', '<=', and '='");
            }

            boolean addWarning = false;
            boolean withTies = false;
            int rankValue = p.right - 1;
            if (p.comparisonOp == predicate.comparisonOperator.EQUAL ||
                p.comparisonOp == predicate.comparisonOperator.LESS_THAN_OR_EQUAL) {
                rankValue = p.right;
            }
            if (functionName.equalsIgnoreCase("RANK") &&
                    (p.comparisonOp == predicate.comparisonOperator.EQUAL && p.right == 1) ||
                    p.comparisonOp == predicate.comparisonOperator.LESS_THAN_OR_EQUAL ||
                    p.comparisonOp == predicate.comparisonOperator.LESS_THAN) {
                withTies = true;
            }
            if (functionName.equalsIgnoreCase("DENSE_RANK") &&
                    (p.comparisonOp == predicate.comparisonOperator.EQUAL && p.right == 1)) {
                withTies = true;
            }
            if ((p.comparisonOp == predicate.comparisonOperator.EQUAL && p.right != 1) ||
                functionName.equals("ROW_NUMBER")) {
                addWarning = true;
            }
            if ((p.comparisonOp == predicate.comparisonOperator.LESS_THAN || p.comparisonOp == predicate.comparisonOperator.LESS_THAN_OR_EQUAL)
                && functionName.equals("DENSE_RANK")) {
                    addWarning = true;
            }
            if (addWarning) {
                StringBuilder builderComment = new StringBuilder();
                builderComment.append("-- the attributes {");
                for (ParserRuleContext attr : partitionByList) {
                    builderComment.append(sqlUtil.getText(attr) + " ");
                }
                for (Pair<ParserRuleContext,Boolean> orderBy : orderByList) {
                    builderComment.append(sqlUtil.getText(orderBy.a) + " ");
                }
                builderComment.append("} has to form Unique Column Combination (UCC), otherwise the rewrite may not be valid.");
                root.addFinalComment(builderComment.toString());
            }
            assert (orderByList.size() > 0);
            wfRank(subqueryString, alias, builder, withTies, rankValue);
        }

        if (functionName.equalsIgnoreCase("LAG") ||
                functionName.equalsIgnoreCase("LEAD")) {
            wfLagLead(subqueryString, builder);
        }

        if (isaAggFunction()) {
            if (range_row_frameBounds.equals("RANGE") ||
                range_row_frameBounds.equals("NONE")) {
                wfRangeAggregate(subqueryString, builder);
            }
            if (range_row_frameBounds.equals("ROWS")) {
                wfRowsAggregate(subqueryString, builder, builder_sub);
            }
        }
        return builder.toString();
    }

    public boolean isaAggFunction() {
        return functionName.equalsIgnoreCase("MAX") ||
                functionName.equalsIgnoreCase("AVG") ||
                functionName.equalsIgnoreCase("MIN") ||
                functionName.equalsIgnoreCase("SUM") ||
                functionName.equalsIgnoreCase("COUNT");
    }

    private void wfRank(String subqueryString,
                        String alias,
                        StringBuilder builder,
                        boolean withTies,
                        int rankValue)
    {
        if (config.getSelectedRankAlgorithm() == Config.rank_algorithm.LateralAgg) {
            // LateralAgg algorithm
            builder.append(" SELECT COUNT(*");
//            if (functionName.equalsIgnoreCase("RANK") ||
//                functionName.equalsIgnoreCase("ROW_NUMBER")) {
//                builder.append("*"); // TODO: order by attributes?
//            }
//            if (functionName.equalsIgnoreCase("DENSE_RANK")) {
//                builder.append("DISTINCT ");
//                buildSimpleListFromOrderBy(builder, false, "");
//            }
            builder.append(") " + winFunAlias);
            builder.append(" FROM (" + subqueryString + ") winfun_subquery");
            if (partitionByList.size() > 0 || orderByList.size() > 0) {
                builder.append(" WHERE ");
            }
            if (partitionByList.size() > 0) {
                buildEqualityWhereCondition(partitionByList, builder, null, "main_subquery");
            }
            if (orderByList.size() > 0) {
                buildOrderByWhereCondition(builder, predicate.comparisonOperator.LESS_THAN_OR_EQUAL);
            }
        }
        else {
            if (isJoinRewrite() && (config.getSelectedRankAlgorithm() == Config.rank_algorithm.JoinMin ||
                    config.getSelectedRankAlgorithm() == Config.rank_algorithm.BestFit)) {
                // JoinMin variant
                if (orderByList.size() > 1) {
                    builder.append(" SELECT * FROM ( SELECT ");
                    buildSimpleListFromPartitionBy(builder, false, "");
                    throw new RuntimeException("There is more than one order by attribute. Unfortunately such rewrite is not implemented yet.");
                    // TODO: finish a situation when there are more than one order by attributes
                } else {
                    // this is a simple situaton when there is only one order by attribute
                    builder.append(" SELECT ");
                    buildSimpleListFromPartitionBy(builder, false, "");
                    if (partitionByList.size() > 0) builder.append(", ");
                    builder.append(" MIN("); // TODO: if ordering is DESC then use MAX
                    String minAttribute = sqlUtil.getText(orderByList.get(0).a).trim();
                    builder.append(minAttribute);
                    builder.append(") min_" + minAttribute + ", 1 " + winFunAlias);
                    builder.append(" FROM (" + subqueryString + ") winfun_subquery");
                    if (partitionByList.size() > 0) {
                        builder.append(" GROUP BY ");
                        buildSimpleListFromPartitionBy(builder, false, "");
                    }
                    // remainder builder
                    StringBuilder remainder_builder = new StringBuilder();
                    buildEqualityWhereCondition(partitionByList, remainder_builder, alias, "main_subquery");
                    if (partitionByList.size() > 0)
                        remainder_builder.append(" AND ");
                    remainder_builder.append(alias + ".min_" + minAttribute + " = main_subquery." + minAttribute);
                    remainder_equality_condition = remainder_builder.toString();
                }
            }
            else if (config.getSelectedRankAlgorithm() == Config.rank_algorithm.JoinNMin) {
                assert (config.getSelectedDbms() == Config.dbms.POSTGRESQL);
                assert (orderByList.size() == 1);
                // JoinNMin variant
                predicate p = predicateList.get(0); // we assume that there is only one predicate (it is controlled in getQueryText method)
                builder.append(" SELECT ");
                buildSimpleListFromPartitionBy(builder, false, "");
                if (partitionByList.size() > 0) builder.append(", ");
                builder.append(" NMIN("); // TODO: if ordering is DESC then use MAX
                String minAttribute = sqlUtil.getText(orderByList.get(0).a).trim();
                builder.append(minAttribute);
                builder.append("," + p.right + ") min_" + minAttribute + ", " + p.right + " " + winFunAlias);
                builder.append(" FROM (" + subqueryString + ") winfun_subquery");
                if (partitionByList.size() > 0) {
                    builder.append(" GROUP BY ");
                    buildSimpleListFromPartitionBy(builder, false, "");
                }
                // remainder builder
                StringBuilder remainder_builder = new StringBuilder();
                buildEqualityWhereCondition(partitionByList, remainder_builder, alias, "main_subquery");
                if (partitionByList.size() > 0)
                    remainder_builder.append(" AND ");
                remainder_builder.append(alias + ".min_" + minAttribute + " = main_subquery." + minAttribute);
                remainder_equality_condition = remainder_builder.toString();
            }
            else {
                if (config.lateralDistinctLimit()) {
                    // LateralDistinctLimit variant
                    builder.append(" SELECT " + winFunAlias);
                    buildSimpleListFromOrderBy(builder, true, "T.");
                    buildSimpleListFromPartitionBy(builder, true, "T.");
                    builder.append(" FROM ( ");
                    builder.append(" SELECT DISTINCT ");
                    buildSimpleListFromPartitionBy(builder, false, "");
                    builder.append(" FROM ( " + subqueryString + " ) T");
                    builder.append(" ) distinct_subquery ");
                    if (config.getSelectedDbms() == Config.dbms.MSSQL || config.getSelectedDbms() == Config.dbms.ORACLE) {
                        builder.append(" OUTER APPLY ( ");
                    } else {
                        builder.append(" LEFT JOIN LATERAL ( ");
                    }
                }
                if (config.getSelectedDbms() == Config.dbms.MSSQL && withTies) {
                    builder.append(" SELECT ");
                    predicate p = predicateList.get(0); // we assume that there is only one predicate (it is controlled in getQueryText method)
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
                    builder.append(" " + rankValue + " " + winFunAlias);
                } else {
                    builder.append(" SELECT " + rankValue + " " + winFunAlias);
                }
                buildSimpleListFromOrderBy(builder, true, "");
                if (config.lateralDistinctLimit()) {
                    buildSimpleListFromPartitionBy(builder, true, "");
                }
                builder.append(" FROM (" + subqueryString + ") winfun_subquery");
                if (partitionByList.size() > 0) {
                    builder.append(" WHERE ");
                    if (config.lateralDistinctLimit()) {
                        buildEqualityWhereCondition(partitionByList, builder, null, "distinct_subquery");
                    } else {
                        buildEqualityWhereCondition(partitionByList, builder, null, "main_subquery");
                    }
                }
                builder.append(" ORDER BY ");
                buildOrderBy(builder, simpleOrdering);
                if (config.getSelectedDbms() != Config.dbms.MSSQL || !withTies) {
                    // MSSQL does not support OFFSET/FETCH WITH TIES, therefore, we use TOP N WITH TIES
                    buildOffsetFetch(builder, functionName, withTies);
                }
                if (config.lateralDistinctLimit()) {
                    if (config.getSelectedDbms() == Config.dbms.MSSQL || config.getSelectedDbms() == Config.dbms.ORACLE) {
                        builder.append(" ) T ");
                    } else {
                        builder.append(" ) T ON true ");
                    }
                }

                // remainder builder
                StringBuilder remainder_builder = new StringBuilder();
                ArrayList<ParserRuleContext> list = new ArrayList<ParserRuleContext>();
                for (int i = 0; i < orderByList.size(); i++) {
                    list.add(orderByList.get(i).a);
                }
                buildEqualityWhereCondition(list, remainder_builder, alias, "main_subquery");
                if (config.lateralDistinctLimit()) {
                    if (orderByList.size() > 0 && partitionByList.size() > 0) {
                        remainder_builder.append(" AND ");
                    }
                    buildEqualityWhereCondition(partitionByList, remainder_builder, alias, "main_subquery");
                }
                remainder_equality_condition = remainder_builder.toString();
            }
        }
    }

    private void wfRowsAggregate(String subqueryString, StringBuilder builder, StringBuilder builder_sub) {
        if (functionExpression == null) {
            builder.append(" SELECT " + functionName + "(*) " + winFunAlias + " FROM ( ");
            builder_sub.append(" SELECT * FROM (" + subqueryString + ") winfun_subquery");
        } else {
            builder.append(" SELECT " + functionName + "(" + sqlUtil.getText(functionExpression) + ") " + winFunAlias + " FROM ( ");
            builder_sub.append(" SELECT " + sqlUtil.getText(functionExpression) + " FROM (" + subqueryString + ") winfun_subquery");
        }
        if (partitionByList.size() > 0 || orderByList.size() > 0) {
            builder_sub.append(" WHERE ");
        }
        if (partitionByList.size() > 0) {
            buildEqualityWhereCondition(partitionByList, builder_sub, null, "main_subquery");
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
        builder.append(" ) winfun_subquery_rows");

    }

     private void wfRangeAggregate(String subqueryString, StringBuilder builder) {
        String funExpression;
        if (functionExpression == null) {
            funExpression = "*";
        } else {
            funExpression = sqlUtil.getText(functionExpression);
        }

        // LateralAgg
        builder.append(" SELECT " + functionName + "(" + funExpression + ") " + winFunAlias);
        builder.append(" FROM (" + subqueryString + ") winfun_subquery");
        if (partitionByList.size() > 0 || orderByList.size() > 0) {
            builder.append(" WHERE ");
        }
        if (partitionByList.size() > 0) {
            buildEqualityWhereCondition(partitionByList, builder, null, "main_subquery");
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
        builder.append(" SELECT " + sqlUtil.getText(functionExpression) + " " + winFunAlias + " FROM (" + subqueryString + ") winfun_subquery");
        if (partitionByList.size() > 0 || orderByList.size() > 0) {
            builder.append(" WHERE ");
        }
        if (partitionByList.size() > 0) {
            buildEqualityWhereCondition(partitionByList, builder, null, "main_subquery");
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
    private void buildOffsetFetch(StringBuilder builder, String functionName, boolean withTies) {
        if (config.getSelectedDbms() == Config.dbms.MYSQL) {
            predicate p = predicateList.get(0);
            if (p.comparisonOp == predicate.comparisonOperator.EQUAL) {
                builder.append(" LIMIT 1");
                builder.append(" OFFSET " + (p.right - 1) + " ");
//                if (withTies) {
//                    builder.append(" WITH TIES ");
//                } else {
//                    builder.append(" ONLY ");
//                }
            }
            if (p.comparisonOp == predicate.comparisonOperator.LESS_THAN || p.comparisonOp == predicate.comparisonOperator.LESS_THAN_OR_EQUAL) {
                int offset = 1;
                if (p.comparisonOp == predicate.comparisonOperator.LESS_THAN_OR_EQUAL) {
                    offset = 0;
                }
                builder.append(" LIMIT " + (p.right - offset));
                builder.append(" OFFSET 0 ");
//                if (withTies) {
//                    builder.append(" WITH TIES ");
//                } else {
//                    builder.append(" ONLY ");
//                }
            }
        }
        else {
            predicate p = predicateList.get(0);
            if (p.comparisonOp == predicate.comparisonOperator.EQUAL) {
                builder.append(" OFFSET " + (p.right - 1) + " ROWS ");
                builder.append(" FETCH FIRST 1 ROWS");
                if (withTies) {
                    builder.append(" WITH TIES ");
                } else {
                    builder.append(" ONLY ");
                }
            }
            if (p.comparisonOp == predicate.comparisonOperator.LESS_THAN || p.comparisonOp == predicate.comparisonOperator.LESS_THAN_OR_EQUAL) {
                int offset = 1;
                if (p.comparisonOp == predicate.comparisonOperator.LESS_THAN_OR_EQUAL) {
                    offset = 0;
                }
                builder.append(" OFFSET 0 ROWS ");
                builder.append(" FETCH FIRST " + (p.right - offset) +" ROWS");
                if (withTies) {
                    builder.append(" WITH TIES ");
                } else {
                    builder.append(" ONLY ");
                }
            }
        }
    }

    /**
     * Builds the WHERE condition from a list of attributes, that needs to be equal.
     * Typicaly it is used when we need a condition for the PARTITION BY clause
     * @param builder
     */
    private void buildEqualityWhereCondition(ArrayList<ParserRuleContext> list, StringBuilder builder, String alias1, String alias2) {
        if (alias1 == null) alias1 = "winfun_subquery";
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
                builder.append(" AND ");
            }
            builder.append("(" + alias1 + "." + sqlUtil.getText(list.get(i)) + " = ");
            builder.append(alias2 + "." + sqlUtil.getText(list.get(i)));
            if (config.getAttributesCanBeNull()) {
                builder.append(" OR (");
                builder.append(alias1 + "." + sqlUtil.getText(list.get(i)) + " IS NULL AND ");
                builder.append(alias2 + "." + sqlUtil.getText(list.get(i)) + " IS NULL)");
            }
            builder.append(")");
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
    private void buildSimpleListFromOrderBy(StringBuilder builder, boolean first_comma, String prefix) {
        for (int i = 0; i < orderByList.size(); i++) {
            if (first_comma || i > 0) {
                builder.append(", ");
            }
            builder.append(prefix + sqlUtil.getText(orderByList.get(i).a));
        }
    }

    /**
     * Builds a list of attributes from the PARTITION BY clause
     * @param builder
     */
    private void buildSimpleListFromPartitionBy(StringBuilder builder, boolean first_comma, String prefix) {
        for (int i = 0; i < partitionByList.size(); i++) {
            if (first_comma || i > 0) {
                builder.append(", ");
            }
            builder.append(prefix + sqlUtil.getText(partitionByList.get(i)));
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
                    if (config.getAttributesCanBeNull())
                    {
                        builder.append(" OR (");
                        builder.append("winfun_subquery." + sqlUtil.getText(orderByList.get(j).a) + " IS NULL AND ");
                        builder.append("main_subquery." + sqlUtil.getText(orderByList.get(j).a) + " IS NULL");
                        builder.append(")");
                    }
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
                if (config.getAttributesCanBeNull()) {
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
                }

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
