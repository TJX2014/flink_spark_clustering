package org.study;

import org.apache.hudi.utilities.HoodieCleaner;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

import static org.study.Constants.TABLE_PATH_TABLE3;

public class CleanJob {

    public static void main(String[] args) {

        JavaSparkContext jsc =
                UtilHelpers.buildSparkContext(ClusterJob.class.getName() + "-hoodie", "local[*]");

        HoodieCleaner.Config config = new HoodieCleaner.Config();
        config.basePath = TABLE_PATH_TABLE3;
        config.configs = new ArrayList<>();
        config.configs.add("hoodie.cleaner.commits.retained=1");
        HoodieCleaner cleaner = new HoodieCleaner(config, jsc);
        cleaner.run();
    }
}
