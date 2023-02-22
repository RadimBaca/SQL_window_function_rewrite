package vsb.baca.grammar.rewriter;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;

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
}
