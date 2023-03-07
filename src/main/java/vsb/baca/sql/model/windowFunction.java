package vsb.baca.sql.model;

import vsb.baca.grammar.rewriter.sqlUtil;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.ArrayList;

public class windowFunction {
    private String functionName;
    private String winFunAlias;
    private String range_row_frameBounds; // possible values are RANGE, ROWS, NONE
    private String frameStart;
    private String frameEnd;
    private ParserRuleContext functionExpression;
    private ParserRuleContext functionContext; // expression element rooting whole window function
    private ArrayList<ParserRuleContext> partitionByList = new ArrayList<ParserRuleContext>();
    private ArrayList<ParserRuleContext> orderByList = new ArrayList<ParserRuleContext>();

    enum comparisonOperator {
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL
    }

    public windowFunction() {
    }

    public windowFunction(String functionName,
                            String winFunAlias,
                          ParserRuleContext functionExpression,
                          ParserRuleContext functionContext) {
        this.functionName = functionName;
        this.winFunAlias = winFunAlias;
        this.functionExpression = functionExpression;
        this.functionContext = functionContext;
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

    public ArrayList<ParserRuleContext> getOrderByList() {
        return orderByList;
    }

    public String getFunctionName() {
        return functionName;
    }

    public ParserRuleContext getFunctionContext() {
        return functionContext;
    }


    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////// Query building methods /////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    public String getQueryText(String subqueryString) {
        StringBuilder builder = new StringBuilder();
        if (functionName.equalsIgnoreCase("MAX") ||
                functionName.equalsIgnoreCase("AVG") ||
                functionName.equalsIgnoreCase("MIN") ||
                functionName.equalsIgnoreCase("SUM") ||
                functionName.equalsIgnoreCase("COUNT")) {
            if (functionExpression == null)
            {
                builder.append(" SELECT " + functionName + "(*) " + winFunAlias);
            } else {
                builder.append(" SELECT " + functionName + "(" + sqlUtil.getText(functionExpression) + ") " + winFunAlias);
            }
            builder.append(" FROM (" + subqueryString + ") AS winfun_subquery");
            if (partitionByList.size() > 0 || orderByList.size() > 0) {
                builder.append(" WHERE ");
            }
            if (partitionByList.size() > 0) {
                buildPartitionByWhereCondition(builder);
            }
            if (orderByList.size() > 0) {
                if (partitionByList.size() > 0) {
                    builder.append(" AND (");
                }
                if (range_row_frameBounds == "RANGE") {
                    comparisonOperator comparisonOp = comparisonOperator.LESS_THAN_OR_EQUAL;
                    buildOrderByWhereCondition(builder, comparisonOp);
                }
                if (partitionByList.size() > 0) {
                    builder.append(")");
                }
            }
        }
        return builder.toString();
    }

    /**
     * Builds the WHERE condition for the partition by clause
     * @param builder
     */
    private void buildPartitionByWhereCondition(StringBuilder builder) {
        for (int i = 0; i < partitionByList.size(); i++) {
            if (i > 0) {
                builder.append(" AND ");
            }
            builder.append("(winfun_subquery." + sqlUtil.getText(partitionByList.get(i)) + " = ");
            builder.append("main_subquery." + sqlUtil.getText(partitionByList.get(i)) + " OR (");
            builder.append("winfun_subquery." + sqlUtil.getText(partitionByList.get(i)) + " IS NULL AND ");
            builder.append("main_subquery." + sqlUtil.getText(partitionByList.get(i)) + " IS NULL))");
        }
    }

    /**
     * Builds the WHERE condition for the order by clause
     * @param builder
     * @param comparisonOp
     */
    private void buildOrderByWhereCondition(StringBuilder builder, comparisonOperator comparisonOp) {
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
                builder.append("winfun_subquery." + sqlUtil.getText(orderByList.get(j)) + " = ");
                builder.append("main_subquery." + sqlUtil.getText(orderByList.get(j)));
                // NULL part
                builder.append(" OR (");
                builder.append("winfun_subquery." + sqlUtil.getText(orderByList.get(j)) + " IS NULL AND ");
                builder.append("main_subquery." + sqlUtil.getText(orderByList.get(j)) + " IS NULL");
                builder.append(")");

                builder.append(")");
            }
            if (i > 0) {
                builder.append(" AND ");
            }
            builder.append("(");
            String comparisonOpString = chooseComparisonOp(comparisonOp, i == orderByList.size() - 1);
            builder.append("winfun_subquery." + sqlUtil.getText(orderByList.get(i)) + comparisonOpString);
            builder.append("main_subquery." + sqlUtil.getText(orderByList.get(i)));
            // NULL part
            builder.append(" OR (");
            if (comparisonOpString.equals("<="))
                builder.append("winfun_subquery." + sqlUtil.getText(orderByList.get(i)) + " IS NULL");
            if (comparisonOpString.equals(">="))
                builder.append("main_subquery." + sqlUtil.getText(orderByList.get(i)) + " IS NULL");
            if (comparisonOpString.equals("<")) {
                builder.append("winfun_subquery." + sqlUtil.getText(orderByList.get(i)) + " IS NULL AND ");
                builder.append("main_subquery." + sqlUtil.getText(orderByList.get(i)) + " IS NOT NULL");
            }
            if (comparisonOpString.equals(">")) {
                builder.append("winfun_subquery." + sqlUtil.getText(orderByList.get(i)) + " IS NOT NULL AND ");
                builder.append("main_subquery." + sqlUtil.getText(orderByList.get(i)) + " IS NULL");
            }
            builder.append(")");

            builder.append(")");
            builder.append(")");
        }
    }

    private String chooseComparisonOp(comparisonOperator comparisonOp, boolean isLast) {
        if (isLast) {
            if (comparisonOp == comparisonOperator.LESS_THAN || comparisonOp == comparisonOperator.LESS_THAN_OR_EQUAL) {
                return "<=";
            } else {
                return ">=";
            }
        } else {
            if (comparisonOp == comparisonOperator.LESS_THAN || comparisonOp == comparisonOperator.LESS_THAN_OR_EQUAL) {
                return "<";
            } else {
                return ">";
            }
        }
    }
}
