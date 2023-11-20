package vsb.baca.sql.model;

import java.util.Locale;

public class Config {
    public enum dbms {
        MSSQL,
        MYSQL,
        POSTGRESQL,
        ORACLE,
        HYPER
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


    public static Config.dbms getSelectedDbms(String dbSystem) {
        dbSystem  = dbSystem.toLowerCase();
        Config.dbms selectedDbms = null;
        if (dbSystem.equals("oracle")) {
            selectedDbms = Config.dbms.ORACLE;
        } else if (dbSystem.equals("postgresql")) {
            selectedDbms = Config.dbms.POSTGRESQL;
        } else if (dbSystem.equals("mssql")) {
            selectedDbms = Config.dbms.MSSQL;
        }
        return selectedDbms;
    }


    public static Config.rank_algorithm getRank_algorithm(String logicalTree) {
        Config.rank_algorithm selectedRankAlgorithm = null;
        logicalTree  = logicalTree.toLowerCase(Locale.ROOT);
        if (logicalTree.equals("lateralagg")) {
            selectedRankAlgorithm = Config.rank_algorithm.LateralAgg;
        } else if (logicalTree.equals("laterallimitties") || logicalTree.equals("laterallimit")) {
            selectedRankAlgorithm = Config.rank_algorithm.LateralLimit;
        } else if (logicalTree.equals("lateraldistinctlimitties") || logicalTree.equals("lateraldistinctlimit")) {
            selectedRankAlgorithm = Config.rank_algorithm.LateralDistinctLimit;
        } else if (logicalTree.equals("joinMin")) {
            selectedRankAlgorithm = Config.rank_algorithm.JoinMin;
        }
        return selectedRankAlgorithm;
    }
}
