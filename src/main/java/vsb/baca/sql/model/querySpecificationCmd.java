package vsb.baca.sql.model;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.tree.ParseTree;
import vsb.baca.grammar.Mssql;
import vsb.baca.grammar.rewriter.sqlUtil;

import java.util.ArrayList;
import java.util.Optional;

/**
 * querySpecificationCmd represents a SELECT statement and inherits from the class selectCmd.
 */
public class querySpecificationCmd extends selectCmd {

    protected Mssql.Query_specificationContext querySpecification;

    protected ArrayList<Pair<String, ParserRuleContext>> selectListElemsOuter = new ArrayList<Pair<String, ParserRuleContext>>(); // select list of the outer query
    protected ArrayList<Pair<String, ParserRuleContext>> selectListElemsOfMainSubquery = new ArrayList<Pair<String, ParserRuleContext>>(); // select list of the main subquery
    protected ArrayList<Pair<String, ParserRuleContext>> selectListElemsUnchanged = new ArrayList<Pair<String, ParserRuleContext>>(); // select list of the subqueries that do not have a window function

    protected ArrayList<windowFunction> windowFunctions = new ArrayList<windowFunction>();


    public querySpecificationCmd(Config config) {
        super(config);
    }

    public querySpecificationCmd(Mssql.Query_specificationContext querySpecification, Config config) {
        super(config);
        this.querySpecification = querySpecification;
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
            this.selectListElemsOuter.add(new Pair("main_subquery", columnAliasContext));
        } else {
            this.selectListElemsOuter.add(new Pair("main_subquery", selectListElem));
        }
        // add into unchanged select list
        this.selectListElemsUnchanged.add(new Pair(null, selectListElem));

        // into the main subquery, we add the full expression
        addIntoSelectList_MainSubquery(selectListElem);
    }

    public void addIntoSelectList_Outer(ParserRuleContext selectListElem) {
        this.selectListElemsOuter.add(new Pair(null, selectListElem));
    }

    /**
     * Method check whether the selectListElem expression is already in the subquery select list.
     * If yes, then do not add it again.
     * @param selectListElem
     */
    public void addIntoSelectList_MainSubquery(ParserRuleContext selectListElem) {
        boolean isInSelectList = false;
        String text2 = selectListElem.getText().trim();
        for  (Pair<String, ParserRuleContext> parserRuleContext : selectListElemsOfMainSubquery) {
            String text1 = parserRuleContext.b.getText().trim();
            if (text1.equals(text2)) { // expressions are compared by their text
                isInSelectList = true;
                break;
            }
        }
        if (!isInSelectList) {
            this.selectListElemsOfMainSubquery.add(new Pair(null, selectListElem));
        }
    }



    @Override public boolean windowFunctionContainsAlias(predicate p) {
        for (windowFunction windowFunction : windowFunctions) {
            if (windowFunction.getWinFunAlias().equals(p.left)) {
                windowFunction.addPredicate(p);
                return true;
            }
        }
        return super.windowFunctionContainsAlias(p);
    }

    public boolean isSubSelectCmd(ParseTree tree) {
        return querySpecification == tree;
    }


    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////// Query building methods /////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////


    /**
     * Main transformed SQL select build method.
     * Window function is transformed into CROSS APPLY.
     * @return
     */
    @Override public String getQueryText() {
        StringBuilder builder = new StringBuilder();
        //getTextWithoutWindowFun(querySpecification);
        if (windowFunctions.size() == 0) {
            return getTextWithoutWindowFun(querySpecification, true);
        }
        String subqueryString = getTextWithoutWindowFun(querySpecification, true);
        builder.append(" SELECT ");
        builder.append(getSelectList(selectListElemsOuter));
        builder.append(" FROM (" + subqueryString + ") AS main_subquery");
        int counter = 1;
        boolean has_remainder = false;
        for (windowFunction windowFunction : windowFunctions) {
            builder.append(getWindowFunctionSubquery(windowFunction, counter++, subqueryString));
            if (windowFunction.getRemainderEqualityCondition() != null)
                has_remainder = true;
        }
        if (has_remainder) {
            builder.append(" WHERE ");
            int remainder_count = 0;
            for (windowFunction windowFunction : windowFunctions) {
                if (windowFunction.getRemainderEqualityCondition() != null) {
                    if (remainder_count++ > 0) builder.append(" AND ");
                    builder.append("(");
                    builder.append(windowFunction.getRemainderEqualityCondition());
                    builder.append(")");
                }
            }
        }
        return builder.toString();
    }



    @Override protected String getTextWithoutWindowFun(ParseTree tree, boolean skip_sql_statement) {
        // check whether the tree is a select list
        if (tree == querySpecification.select_list()) {
            if (windowFunctions.size() == 0) {
                return " " + getSelectList(selectListElemsUnchanged);
            } else {
                return " " + getSelectList(selectListElemsOfMainSubquery);
            }
        }
        return super.getTextWithoutWindowFun(tree, skip_sql_statement);
    }


    private String getSelectList(ArrayList<Pair<String, ParserRuleContext>> selectList) {
        StringBuilder builder = new StringBuilder();
        for (Pair<String, ParserRuleContext> selectListElem : selectList) {
            builder.append(" ");
            if (selectListElem.a != null)
                builder.append(selectListElem.a + ".");
            builder.append(getTextWithoutWindowFun(selectListElem.b, false));
            if (selectListElem != selectList.get(selectList.size() - 1)) {
                builder.append(",");
            }
        }
        return builder.toString();
    }


    private String getWindowFunctionSubquery(windowFunction windowFunction, int counter, String subqueryString) {
        StringBuilder builder = new StringBuilder();
        String alias = "win_subquery_" + counter;
        // if window function is rank or dense_rank, the order by clouse attributes are not null,
        // and rank equals to one, than use JOIN
        if (windowFunction.isJoinRewrite()) {
            builder.append(" JOIN (" + windowFunction.getQueryText(subqueryString, alias) + ") AS " + alias + " ON " + windowFunction.getRemainderEqualityCondition());
            windowFunction.resetRemainderEqualityCondition();
        } else {
            builder.append(" OUTER APPLY (" + windowFunction.getQueryText(subqueryString, alias) + ") AS " + alias);
        }
        return builder.toString();
    }

}