package vsb.baca.sql.model;

public class Config {
    public enum dbms {
        MSSQL,
        MYSQL,
        POSTGRESQL,
        ORACLE
    }

    public enum rank_algorithm {
        LateralAgg,
        LateralLimit,
        LateralDistinctLimit,
        JoinMin,
        JoinNMin,
        BestFit // Best fit do not consider the JoinNMin algorithm
    }

    private dbms selectedDbms;
    private rank_algorithm selectedRankAlgorithm; // use this algortihm if possible
    private boolean attributes_can_be_null = true;

    public Config(dbms selectedDbms, boolean attributes_can_be_null, rank_algorithm rank_algorithm) {
        this.selectedDbms = selectedDbms;
        this.attributes_can_be_null = attributes_can_be_null;
        this.selectedRankAlgorithm = rank_algorithm;
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

    public rank_algorithm getSelectedRankAlgorithm() {
        return selectedRankAlgorithm;
    }

    public boolean lateralDistinctLimit() {
        return selectedRankAlgorithm == rank_algorithm.LateralDistinctLimit ||
                selectedRankAlgorithm == rank_algorithm.JoinMin ||
                selectedRankAlgorithm == rank_algorithm.BestFit;
    }

    public void setRankAlgorithm(rank_algorithm rankAlg) {
        selectedRankAlgorithm = rankAlg;
    }
}
