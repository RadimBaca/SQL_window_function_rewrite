package vsb.baca.sql.benchmark;


import java.util.ArrayList;

public class run_setup {
    public String tab_prefix;
    public ArrayList<String> queryFileNames;
    public String option_maxdop;
    public bench_config.Padding padding;
    public bench_config.Storage storage;
    public bench_config.Parallelism parallelism;

    public run_setup(String tab_prefix,
                     ArrayList<String> queryFileNames,
                     String option_maxdop,
                     bench_config.Padding padding,
                     bench_config.Storage storage,
                     bench_config.Parallelism parallelism) {
        this.tab_prefix = tab_prefix;
        this.queryFileNames = queryFileNames;
        this.option_maxdop = option_maxdop;
        this.padding = padding;
        this.storage = storage;
        this.parallelism = parallelism;
    }
}
