package vsb.baca.sql.model;

public class Config {
    public enum dbms {
        MSSQL,
        MYSQL,
        POSTGRESQL,
        ORACLE
    }

    private dbms selectedDbms;
    private boolean attributes_can_be_null = true;

    public Config(dbms selectedDbms, boolean attributes_can_be_null) {
        this.selectedDbms = selectedDbms;
        this.attributes_can_be_null = attributes_can_be_null;
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
}
