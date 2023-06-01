package vsb.baca.sql.model;

public class Config {
    public enum dbms {
        MSSQL,
        MYSQL,
        POSTGRESQL,
        ORACLE
    }

    private dbms selectedDbms;
    private boolean rank_rewrite_is_double_subquery = true; // if true, then the rank rewrite with cross apply lead to two subqueries
    private boolean attributes_can_be_null = true;

    public Config(dbms selectedDbms, boolean attributes_can_be_null, boolean rank_rewrite_is_double_subquery) {
        this.selectedDbms = selectedDbms;
        this.attributes_can_be_null = attributes_can_be_null;
        this.rank_rewrite_is_double_subquery = rank_rewrite_is_double_subquery;
    }

    public dbms getSelectedDbms() {
        return selectedDbms;
    }

    public void setAttributesCanBeNull(boolean attributes_can_be_null) {
        this.attributes_can_be_null = attributes_can_be_null;
    }

    public boolean getAttributesCanBeNull() {
        return attributes_can_be_null;
    }

    public boolean getRankRewriteIsDoubleSubquery() {
        return rank_rewrite_is_double_subquery;
    }
}
