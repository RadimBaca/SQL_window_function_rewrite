package vsb.baca.sql.benchmark;

import vsb.baca.sql.model.Config;

import java.util.ArrayList;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class bench_config {
    public String DROPINDEXES_FILENAME;
    public String CREATEINDEXES_FILENAME;
    public String CONNECTION_STRING;
    public Config config;
    public Logger logger;
    public FileHandler fileHandler;
    public ArrayList<String> queryFileNames;
    public String tab_prefix;
    public Storage storage; // "row" or "column"
    public Padding padding; // "padding_on" or "padding_off"
    public Parallelism parallelism; // "parallelism_on" or "parallelism_off"

    public enum Storage {
        ROW,
        COLUMN
    }

    public enum Padding {
        ON,
        OFF
    }

    public enum Parallelism {
        ON,
        OFF
    }

    public bench_config(String createindexes_filename,
                        String dropindexes_filename,
                         String connection_string,
                         Config config,
                         Logger logger,
                         FileHandler fileHandler,
                         ArrayList<String> queryFileNames,
                        String tab_prefix,
                        Storage storage,
                        Padding padding,
                        Parallelism parallelism) {
        this.CREATEINDEXES_FILENAME = createindexes_filename;
        this.DROPINDEXES_FILENAME = dropindexes_filename;
        this.CONNECTION_STRING = connection_string;
        this.config = config;
        this.logger = logger;
        this.fileHandler = fileHandler;
        this.queryFileNames = queryFileNames;
        this.tab_prefix = tab_prefix;
        this.storage = storage;
        this.padding = padding;
        this.parallelism = parallelism;
    }
}
