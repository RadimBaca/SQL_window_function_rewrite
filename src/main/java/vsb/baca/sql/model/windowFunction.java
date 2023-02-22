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
            for (int i = 0; i < partitionByList.size(); i++) {
                if (i > 0) {
                    builder.append(" AND ");
                }
                builder.append("winfun_subquery." + sqlUtil.getText(partitionByList.get(i)) + " = ");
                builder.append("main_subquery." + sqlUtil.getText(partitionByList.get(i)));
            }
            if (orderByList.size() > 0) {
                if (partitionByList.size() > 0) {
                    builder.append(" AND ");
                }
                for (int i = 0; i < orderByList.size(); i++) {
                    if (i > 0) {
                        builder.append(" AND ");
                    }
                    if (range_row_frameBounds == "RANGE") {
                        if (frameStart == "CURRENT") {
                            // frame start is current row
                            builder.append("(winfun_subquery." + sqlUtil.getText(orderByList.get(i)) + " >= ");
                            builder.append("main_subquery." + sqlUtil.getText(orderByList.get(i))+ " OR ");
                            builder.append("main_subquery." + sqlUtil.getText(orderByList.get(i)) + " IS NULL)");
                        } else if (frameStart != "UNBOUNDED") {
                            // frame start is a number
                            builder.append("(winfun_subquery." + sqlUtil.getText(orderByList.get(i)) + " >= ");
                            builder.append("main_subquery." + sqlUtil.getText(orderByList.get(i)) + " - " + frameStart+ " OR ");
                            builder.append("main_subquery." + sqlUtil.getText(orderByList.get(i)) + " IS NULL)");
                        }
                        if (frameEnd == "CURRENT") {
                            // frame end is current row
                            builder.append("(winfun_subquery." + sqlUtil.getText(orderByList.get(i)) + " <= ");
                            builder.append("main_subquery." + sqlUtil.getText(orderByList.get(i))+ " OR ");
                            builder.append("winfun_subquery." + sqlUtil.getText(orderByList.get(i)) + " IS NULL)");
                        } else if (frameEnd != "UNBOUNDED") {
                            // frame end is a number
                            builder.append("(winfun_subquery." + sqlUtil.getText(orderByList.get(i)) + " <= ");
                            builder.append("main_subquery." + sqlUtil.getText(orderByList.get(i)) + " + " + frameEnd + " OR ");
                            builder.append("winfun_subquery." + sqlUtil.getText(orderByList.get(i)) + " IS NULL)");
                        }
                    }
                }
            }
        }
        return builder.toString();
    }
}
