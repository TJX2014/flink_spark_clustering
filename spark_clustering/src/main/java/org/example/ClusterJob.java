package org.example;

import org.apache.hudi.utilities.HoodieClusteringJob;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.spark.api.java.JavaSparkContext;

public class ClusterJob {
    public static String TABLE_PATH_TABLE1 = "file:///C://Users/Allen/Desktop/warehouse/clustering/t1";

    public static String TABLE_PATH_TABLE2 = "file:///C://Users/Allen/Desktop/warehouse/t2";
    public static String WAREHOUSE_BASE_PATH = "file:///C://Users/Allen/Desktop/warehouse";
    public static void main(String[] args) {
        HoodieClusteringJob.Config clusterClusteringConfig = buildHoodieClusteringUtilConfig(
                TABLE_PATH_TABLE1
                 ,null
                 , true,
//                "scheduleandexecute",
                "execute",
                true);
        JavaSparkContext jsc =
                UtilHelpers.buildSparkContext(ClusterJob.class.getName() + "-hoodie", "local[*]");
        HoodieClusteringJob clusterClusteringJob = new HoodieClusteringJob(jsc, clusterClusteringConfig);
        clusterClusteringJob.cluster(clusterClusteringConfig.retry);
    }

    private static HoodieClusteringJob.Config buildHoodieClusteringUtilConfig(String basePath,
                                                                       String clusteringInstantTime,
                                                                       Boolean runSchedule,
                                                                       String runningMode,
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
