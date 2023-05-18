package vsb.baca.grammar.rewriter;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import vsb.baca.sql.model.predicate;
import vsb.baca.sql.model.windowFunction;
import vsb.baca.grammar.Mssql;

import java.util.ArrayList;
import java.util.Locale;

public class sqlUtil {
    public static String getText(ParseTree tree) {
        if (tree.getChildCount() == 0) {
            return " " + tree.getText();
        } else {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < tree.getChildCount(); i++) {
                ParseTree child = tree.getChild(i);
                if (child.getChildCount() > 0) {
                    builder.append(getText(child));
                } else {
                    builder.append(" " + child.getText());
                }
            }
            return builder.toString();
        }
    }

    public static <T> void traverseTree(ParseTree tree, Class<T> subclass, ArrayList<ParserRuleContext> list) {
        if (tree.getChildCount() == 0) {
            if (subclass.isInstance(tree)) {
                list.add((ParserRuleContext) tree);
            }
        } else {
            for (int i = 0; i < tree.getChildCount(); i++) {
                ParseTree child = tree.getChild(i);
                if (subclass.isInstance(child)) {
                    list.add((ParserRuleContext) child);
                } else {
                    if (child.getChildCount() > 0) {
                        traverseTree(child, subclass, list);
                    }
                }
            }
        }
    }

    public static boolean isNumber(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static int parseFrameBound(String str) {
        try {
            int bound = Integer.parseInt(str);
            return bound;
        } catch (NumberFormatException e) {
            if (str.toLowerCase(Locale.ROOT).equals("current")) return 0;
            else if (str.toLowerCase(Locale.ROOT).equals("unbounded")) return Integer.MIN_VALUE;
            assert(true);
            return Integer.MAX_VALUE;
        }
    }

    public static predicate.comparisonOperator getComparisonOperator(Mssql.Comparison_operatorContext ctx) {
        if (ctx.getChild(0).getText().equals("=")) return predicate.comparisonOperator.EQUAL;
        else if (ctx.getChild(0).getText().equals("<")) return predicate.comparisonOperator.LESS_THAN;
        else if (ctx.getChild(0).getText().equals("<=")) return predicate.comparisonOperator.LESS_THAN_OR_EQUAL;
        else if (ctx.getChild(0).getText().equals(">")) return predicate.comparisonOperator.GREATER_THAN;
        else if (ctx.getChild(0).getText().equals(">=")) return predicate.comparisonOperator.GREATER_THAN_OR_EQUAL;
        return null;
    }
}
