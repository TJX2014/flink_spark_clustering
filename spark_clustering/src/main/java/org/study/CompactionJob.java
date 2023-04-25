package org.study;

import org.apache.hudi.table.action.compact.strategy.LogFileNumBasedCompactionStrategy;
import org.apache.hudi.utilities.HoodieCompactor;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.spark.api.java.JavaSparkContext;

import static org.study.Constants.TABLE_PATH_TABLE;

public class CompactionJob {

    public static void main(String[] args) throws Exception {
        final HoodieCompactor.Config cfg = new HoodieCompactor.Config();
        cfg.basePath = TABLE_PATH_TABLE;
        cfg.runningMode = "scheduleAndExecute";
//        cfg.runningMode = "execute";
        cfg.strategyClassName = LogFileNumBasedCompactionStrategy.class.getName();
        final JavaSparkContext jsc = UtilHelpers.buildSparkContext("compactor-" + "t1", "local[*]", "1g");

        HoodieCompactor compactor = new HoodieCompactor(jsc, cfg);
        int ret = compactor.compact(1);
        System.out.println(ret);
    }
}
