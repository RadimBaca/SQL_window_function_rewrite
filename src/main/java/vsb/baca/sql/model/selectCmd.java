package vsb.baca.sql.model;

import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import vsb.baca.grammar.Mssql;
import vsb.baca.grammar.rewriter.sqlUtil;
import vsb.baca.sql.model.predicate;

import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * selectCmd represents a SELECT statement. It may contains several SELECT statements interconnected by UNION, INTERSECT, EXCEPT.
 */
public class selectCmd {
    protected Mssql.Select_statementContext select_statement;
    protected ArrayList<selectCmd> subSelectCmds = new ArrayList<selectCmd>(); // SELECT subqueries of the current query
    protected Config config;

    public selectCmd(Config config) {
        this.config = config;
    }

    public selectCmd(Mssql.Select_statementContext selectStatement, Config config) {
        this.select_statement = selectStatement;
        this.config = config;
    }

    public void addSubSelectCmd(selectCmd subSelectCmd) {
        this.subSelectCmds.add(subSelectCmd);
    }

    public boolean windowFunctionContainsAlias(predicate p) {
        for (selectCmd subSelectCmd : subSelectCmds) {
            if (subSelectCmd.windowFunctionContainsAlias(p)) {
                return true;
            }
        }
        return false;
    }


    public boolean isSubSelectCmd(ParseTree tree) {
        return select_statement == tree;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////// Query building methods /////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Main transform method. It recursively rewrites the query and all its subqueries.
     * @return
     */
    public String getQueryText() {
        return getTextWithoutWindowFun(select_statement, true);
    }


    protected String getTextWithoutWindowFun(ParseTree tree, boolean skip_sql_statement) {
        if (!skip_sql_statement && (tree instanceof Mssql.Query_specificationContext || tree instanceof Mssql.Select_statementContext)) {
            // find selectCmd in subSelectCmds where the select_statement is the same as the tree
            Optional<selectCmd> first = subSelectCmds.stream().filter(x -> x.isSubSelectCmd(tree)).findFirst();
            selectCmd subSelectCmd = first.get();
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
