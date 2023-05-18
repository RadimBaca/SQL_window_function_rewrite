package vsb.baca.sql.model;

public class predicate {
    public String left;
    public int right;
    public comparisonOperator comparisonOp;

    public predicate(String left, int right, comparisonOperator comparisonOp) {
        this.left = left;
        this.right = right;
        this.comparisonOp = comparisonOp;
    }

    public enum comparisonOperator {
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        EQUAL
    }

    public void swapComparisonOperator() {
        switch (comparisonOp) {
            case LESS_THAN:
                comparisonOp = comparisonOperator.GREATER_THAN;
                break;
            case LESS_THAN_OR_EQUAL:
                comparisonOp = comparisonOperator.GREATER_THAN_OR_EQUAL;
                break;
            case GREATER_THAN:
                comparisonOp = comparisonOperator.LESS_THAN;
                break;
            case GREATER_THAN_OR_EQUAL:
                comparisonOp = comparisonOperator.LESS_THAN_OR_EQUAL;
                break;
            case EQUAL:
                comparisonOp = comparisonOperator.EQUAL;
                break;
        }
    }
}
