package vsb.baca.grammar.rewriter;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import vsb.baca.grammar.Mssql;
import vsb.baca.grammar.MssqlBaseVisitor;
import vsb.baca.sql.model.selectCmd;
import vsb.baca.sql.model.windowFunction;

import java.util.ArrayList;
import java.util.Stack;

public class Mssql_rewriter_visitor<T> extends MssqlBaseVisitor<T> {
    private selectCmd selectCmds;
    private Stack<selectCmd> selectCmdStack = new Stack<selectCmd>();
    private boolean is_window_function_element = false;

    @Override public T visitSelect_statement(Mssql.Select_statementContext ctx) {
        selectCmd selectCmd = new selectCmd(ctx.query_expression().query_specification());
        if (!selectCmdStack.empty()) {
            selectCmdStack.peek().addSubSelectCmd(selectCmd);
        }
        selectCmdStack.push(selectCmd);
        T item = visitChildren(ctx);
        selectCmdStack.pop();
        selectCmds = selectCmd;
        return item;
    }

    @Override public T visitSelect_list_elem(Mssql.Select_list_elemContext ctx) {
        boolean remember_window_function_element = is_window_function_element;
        T item = visitChildren(ctx);
        // push select list element (without window functions)
        if (!is_window_function_element) {
            selectCmdStack.peek().addIntoSelectList_All(ctx);
        }
        is_window_function_element = remember_window_function_element;
        return item;
    }

    @Override public T visitOrder_by_expression(Mssql.Order_by_expressionContext ctx) {
        selectCmdStack.peek().addIntoSelectList_MainSubquery(ctx);
        return visitChildren(ctx);
    }

    /**
     * Visit window function. We assume that window function has only attributes in over clause.
     * @param ctx
     * @return
     */
    @Override public T visitAggregate_windowed_function(Mssql.Aggregate_windowed_functionContext ctx) {
        if (ctx.over_clause() != null) {
            // push only window functions (with over clause)
            selectCmd actualSelectCmd = selectCmdStack.peek();
            ParserRuleContext aliasContext = (ParserRuleContext)ctx.getParent().getParent().getParent().getChild(1);
            String winFunAlias = sqlUtil.getText(aliasContext);
            windowFunction function = null;
            if (ctx.agg_func != null) {
                String agg_fun = ctx.agg_func.getText().trim();
                actualSelectCmd.addIntoSelectList_MainSubquery(ctx.all_distinct_expression());
                function = new windowFunction(agg_fun, winFunAlias, ctx.all_distinct_expression(), ctx.getParent().getParent().getParent());
            }
            if (ctx.cnt != null) {
                String agg_fun = ctx.cnt.getText().trim();
                if (ctx.all_distinct_expression() != null) {
                    function = new windowFunction(agg_fun, winFunAlias, ctx.all_distinct_expression(), ctx.getParent().getParent().getParent());
                } else {
                    function = new windowFunction(agg_fun, winFunAlias, null, ctx.getParent().getParent().getParent());
                }
            }
            // collect partition by expressions
            function.setRange_row_frameBounds("NONE");
            if (ctx.over_clause().expression_list() != null) {
                sqlUtil.traverseTree(ctx.over_clause().expression_list(), Mssql.ExpressionContext.class, function.getPartitionByList());
            }
            // collect order by expressions
            if (ctx.over_clause().order_by_clause() != null) {
                sqlUtil.traverseTree(ctx.over_clause().order_by_clause(), Mssql.ExpressionContext.class, function.getOrderByList());
                // implicit frame bounds settings
                function.setRange_row_frameBounds("RANGE");
                function.setFrameStart("UNBOUNDED");
                function.setFrameEnd("CURRENT");
            }
            // resolve frame bounds
            if (ctx.over_clause().row_or_range_clause() != null) {
                function.setRange_row_frameBounds(ctx.over_clause().row_or_range_clause().getChild(0).getText().trim());
                ArrayList<ParserRuleContext> frameBounds = new ArrayList<ParserRuleContext>();
                sqlUtil.traverseTree(ctx.over_clause().row_or_range_clause(), Mssql.Window_frame_boundContext.class, frameBounds);
                if (frameBounds.size() == 2) { // frame bounds are defined using BETWEEN X AND Y
                    // this adds the frame bounds to the main subquery select list, the SQL Server supports just constants
                    int i = 0;
                    for (ParserRuleContext frameBound : frameBounds) {
                        Mssql.Window_frame_boundContext startBound = ((Mssql.Window_frame_boundContext) frameBound);
                        String stringBound = "";
                        if (startBound.window_frame_preceding() != null) {
                            if (startBound.window_frame_preceding().frame_start.getText().trim() != "UNBOUNDED" &&
                                    startBound.window_frame_preceding().frame_start.getText().trim() != "CURRENT") {
                                stringBound = "- " + startBound.window_frame_preceding().frame_start.getText().trim();
                            } else {
                                stringBound = startBound.window_frame_preceding().frame_start.getText().trim();
                            }
                        } else {
                            assert startBound.window_frame_following() != null;
                            stringBound = startBound.window_frame_following().frame_start.getText().trim();
                        }
                        if (i == 0) {
                            function.setFrameStart(stringBound);
                        } else {
                            function.setFrameEnd(stringBound);
                        }
                        i++;
                    }
                } else { // frame bounds are defined using UNBOUNDED PRECEDING, CURRENT ROW
                    sqlUtil.traverseTree(ctx.over_clause().row_or_range_clause(), Mssql.Window_frame_extentContext.class, frameBounds);
                    if (frameBounds.size() == 1) { // frame bounds are defined using UNBOUNDED PRECEDING, CURRENT ROW
                        function.setFrameStart(((Mssql.Window_frame_extentContext)frameBounds.get(0)).window_frame_preceding().frame_start.getText().trim());
                        function.setFrameEnd("CURRENT");
                    } else {
                        throw new RuntimeException("Frame bounds are defined in an unsupported way.");
                    }
                }
            }


            actualSelectCmd.addWindowFunction(function);
            actualSelectCmd.addIntoSelectList_Outer(aliasContext);
            is_window_function_element = true;
            // add partition by and order by expressions to the main subquery select list
            for (ParserRuleContext expressionContext : function.getPartitionByList()) {
                actualSelectCmd.addIntoSelectList_MainSubquery(expressionContext);
            }
            for (ParserRuleContext expressionContext : function.getOrderByList()) {
                actualSelectCmd.addIntoSelectList_MainSubquery(expressionContext);
            }
        }
        return visitChildren(ctx);
    }

    public selectCmd getSelectCmd() {
        return selectCmds;
    }
}
