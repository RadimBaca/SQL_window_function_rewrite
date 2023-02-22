package vsb.baca.sql.model;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import vsb.baca.grammar.Mssql;
import vsb.baca.grammar.rewriter.sqlUtil;

import java.util.ArrayList;

public class selectCmd {
    private Mssql.Query_specificationContext querySpecification;
    private Mssql.Select_statementContext select_statement;

    private ArrayList<ParserRuleContext> selectListElems = new ArrayList<ParserRuleContext>();
    private ArrayList<ParserRuleContext> selectListElemsOfMainSubquery = new ArrayList<ParserRuleContext>();

    private ArrayList<windowFunction> windowFunctions = new ArrayList<windowFunction>();
    private ArrayList<selectCmd> subSelectCmds = new ArrayList<selectCmd>();

    public selectCmd() {
    }

    public selectCmd(Mssql.Query_specificationContext querySpecification) {
        this.querySpecification = querySpecification;
        this.select_statement = (Mssql.Select_statementContext)querySpecification.getParent().getParent();
    }

    public void addWindowFunction(windowFunction windowFunction) {
        this.windowFunctions.add(windowFunction);
    }

    /**
     * Add the selectListElem expression into the select list of the outer query as well as subquery.
     * If the expression has alias, then use only the alias in the select list of outer query.
     * In the subquery, we add the full expression however only if it is not already there.
     * @param selectListElem
     */
    public void addIntoSelectList_All(Mssql.Select_list_elemContext selectListElem) {
        ArrayList<ParserRuleContext> columnAliasContexts = new ArrayList<ParserRuleContext>();
        sqlUtil.traverseTree(selectListElem, Mssql.Column_aliasContext.class, columnAliasContexts);
        if (columnAliasContexts.size() > 0) {
            // if the expression has alias, then use only the alias in the select list of outer query
            Mssql.Column_aliasContext columnAliasContext = (Mssql.Column_aliasContext) columnAliasContexts.get(0);
            this.selectListElems.add(columnAliasContext);
        } else {
            this.selectListElems.add(selectListElem);
        }
        // into the main subquery, we add the full expression
        addIntoSelectList_MainSubquery(selectListElem);
    }

    public void addIntoSelectList_Outer(ParserRuleContext selectListElem) {
        this.selectListElems.add(selectListElem);
    }



    /**
     * Method check whether the selectListElem expression is already in the subquery select list.
     * If yes, then do not add it again.
     * @param selectListElem
     */
    public void addIntoSelectList_MainSubquery(ParserRuleContext selectListElem) {
        boolean isInSelectList = false;
        for  (ParserRuleContext parserRuleContext : selectListElemsOfMainSubquery) {
            String text1 = parserRuleContext.getText().trim();
            String text2 = selectListElem.getText().trim();
            if (text1.equals(text2)) { // expressions are compared by their text
                isInSelectList = true;
                break;
            }
        }
        if (!isInSelectList) {
            this.selectListElemsOfMainSubquery.add(selectListElem);
        }
    }

    public void addSubSelectCmd(selectCmd subSelectCmd) {
        this.subSelectCmds.add(subSelectCmd);
    }

    public Mssql.Select_statementContext getSelectStatement() {
        return select_statement;
    }

    public boolean isSubSelectCmd(ParseTree tree) {
        return select_statement == tree;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////// Query building methods /////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Main transformed SQL select build method.
     * Window function is transformed into CROSS APPLY.
     * @return
     */
    public String getQueryText() {
        StringBuilder builder = new StringBuilder();
        //getTextWithoutWindowFun(querySpecification);
        if (windowFunctions.size() == 0) {
            return getTextWithoutWindowFun(select_statement, true);
        }
        String subqueryString = getTextWithoutWindowFun(querySpecification, false);
        builder.append(" SELECT ");
        builder.append(getSelectListOfQuery());
        builder.append(" FROM (" + subqueryString + ") AS main_subquery");
        int counter = 1;
        for (windowFunction windowFunction : windowFunctions) {
            builder.append(getWindowFunctionCrossApply(windowFunction, counter++, subqueryString));
        }
        if (select_statement.select_order_by_clause() != null) {
            builder.append(" " + getTextWithoutWindowFun(select_statement.select_order_by_clause(), false));
        }
        return builder.toString();
    }

    private String getWindowFunctionCrossApply(windowFunction windowFunction, int counter, String subqueryString) {
        StringBuilder builder = new StringBuilder();
        builder.append(" CROSS APPLY (" + windowFunction.getQueryText(subqueryString) + ") AS subquery_" + counter);
        return builder.toString();
    }

    /**
     * Build the select list of the main subquery
     * @return String of the select list
     */
    private String getSelectListOfMainSubquery() {
        StringBuilder builder = new StringBuilder();
        for (ParserRuleContext selectListElem : selectListElemsOfMainSubquery) {
            builder.append(" " + getTextWithoutWindowFun(selectListElem, false));
            if (selectListElem != selectListElemsOfMainSubquery.get(selectListElemsOfMainSubquery.size() - 1)) {
                builder.append(",");
            }
        }
        return builder.toString();
    }

    /**
     * Build the select list of the select query
     * @return String of the select list
     */
    private String getSelectListOfQuery() {
        StringBuilder builder = new StringBuilder();
        for (ParserRuleContext selectListElem : selectListElems) {
            builder.append(" " + getTextWithoutWindowFun(selectListElem, false));
            if (selectListElem != selectListElems.get(selectListElems.size() - 1)) {
                builder.append(",");
            }
        }
        return builder.toString();
    }

    private String getTextWithoutWindowFun(ParseTree tree, boolean skip_sql_statement) {
        // check whether the tree is a select list
        if (tree == querySpecification.select_list()) {
            if (windowFunctions.size() == 0) {
                return " " + getSelectListOfQuery();
            } else {
                return " " + getSelectListOfMainSubquery();
            }
        }
        if (!skip_sql_statement && tree instanceof Mssql.Select_statementContext) {
            // find subSelectCmd in subSelectCmds where the select_statement is the same as the tree
            selectCmd subSelectCmd = subSelectCmds.stream().filter(x -> x.isSubSelectCmd(tree)).findFirst().get();
            return " " + subSelectCmd.getQueryText();
        }

        if (tree.getChildCount() == 0) {
            return " " + tree.getText();
        } else {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < tree.getChildCount(); i++) {
                ParseTree child = tree.getChild(i);
                if (child.getChildCount() > 0) {
                    builder.append(getTextWithoutWindowFun(child, false));
                } else {
                    builder.append(" " + child.getText());
                }
            }
            return builder.toString();
        }
    }
}
