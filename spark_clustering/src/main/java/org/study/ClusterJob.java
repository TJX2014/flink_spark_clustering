package org.study;

import org.apache.hudi.utilities.HoodieClusteringJob;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.study.Constants.TABLE_PATH_TABLE3;

public class ClusterJob {

    private static Logger LOG = LoggerFactory.getLogger(ClusterJob.class);
    public static String TABLE_PATH_TABLE2 = "file:///C://Users/Allen/Desktop/warehouse/t2";
    public static String WAREHOUSE_BASE_PATH = "file:///C://Users/Allen/Desktop/warehouse";
    public static void main(String[] args) throws InterruptedException {
        HoodieClusteringJob.Config clusterClusteringConfig = buildHoodieClusteringUtilConfig(
                TABLE_PATH_TABLE3
                 ,null
                 , true,
//                "execute",
//                "scheduleandexecute",
//                "schedule",
                "schedule",
//                "execute",
                true);
        JavaSparkContext jsc =
                UtilHelpers.buildSparkContext(ClusterJob.class.getName() + "-hoodie", "local[*]");

        HoodieClusteringJob clusterClusteringJob = new HoodieClusteringJob(jsc, clusterClusteringConfig);
        while (true) {
            clusterClusteringJob.cluster(clusterClusteringConfig.retry);
            Thread.sleep(1000);
        }
    }

    private static HoodieClusteringJob.Config buildHoodieClusteringUtilConfig(String basePath,
                                                                       String clusteringInstantTime,
                                                                       Boolean runSchedule,String runningMode,
                                                                       Boolean retryLastFailedClusteringJob) {
        HoodieClusteringJob.Config config = new HoodieClusteringJob.Config();
        config.basePath = basePath;
        config.clusteringInstantTime = clusteringInstantTime;
        config.runSchedule = runSchedule;
        config.propsFilePath = WAREHOUSE_BASE_PATH + "/clusteringjob.properties";
        config.runningMode = runningMode;
        if (retryLastFailedClusteringJob != null) {
            config.retryLastFailedClusteringJob = retryLastFailedClusteringJob;
        }
        return config;
    }
}
