package vsb.baca.sql.benchmark;

import org.antlr.v4.runtime.misc.Pair;
import vsb.baca.sql.model.Config;

import java.util.ArrayList;

public class run_setup {
    public String tab_prefix;
    public ArrayList<Pair<String, String>> queryFileNames;
    public String option_maxdop; // TODO remove
    public bench_config.Padding padding;
    public bench_config.Storage storage;
    public bench_config.Parallelism parallelism;
    public Config config;
    public String username;
    public String password;
    public String connection_string;

    public run_setup(String tab_prefix,
                     ArrayList<Pair<String, String>> queryFileNames,
                     String option_maxdop,
                     bench_config.Padding padding,
                     bench_config.Storage storage,
                     bench_config.Parallelism parallelism,
                     Config config) {
        this.tab_prefix = tab_prefix;
        this.queryFileNames = queryFileNames;
        this.option_maxdop = option_maxdop;
        this.padding = padding;
        this.storage = storage;
        this.parallelism = parallelism;
        this.config = config;
    }


    public run_setup(String tab_prefix,
                     ArrayList<Pair<String, String>> queryFileNames,
                     String option_maxdop,
                     bench_config.Padding padding,
                     bench_config.Storage storage,
                     bench_config.Parallelism parallelism,
                     Config config,
                     String connection_string,
                     String username,
                     String password) {
        this.tab_prefix = tab_prefix;
        this.queryFileNames = queryFileNames;
        this.option_maxdop = option_maxdop;
        this.padding = padding;
        this.storage = storage;
        this.parallelism = parallelism;
        this.config = config;
        this.username = username;
        this.password = password;
        this.connection_string = connection_string;
    }
}
