package org.study;

import org.apache.hudi.utilities.HoodieClusteringJob;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterJobWithConsistentBucket {

    private static Logger LOG = LoggerFactory.getLogger(ClusterJobWithConsistentBucket.class);
    public static void main(String[] args) throws InterruptedException {

        HoodieClusteringJob.Config config = new HoodieClusteringJob.Config();
        config.basePath = "file:///C://Users/Allen/Desktop/warehouse/clustering3/t4";
        config.runSchedule = true;
        config.runningMode = "scheduleAndExecute";
        config.configs.add("hoodie.index.type=BUCKET");
        config.configs.add("hoodie.datasource.write.recordkey.field=uuid");
        config.configs.add("hoodie.bucket.index.min.num.buckets=3");
        config.configs.add("hoodie.bucket.index.max.num.buckets=5");
        config.configs.add("hoodie.parquet.max.file.size=3145728");
        config.configs.add("hoodie.bucket.index.hash.field=uuid");
        config.configs.add("hoodie.index.bucket.engine=CONSISTENT_HASHING");
        config.configs.add("hoodie.clustering.plan.strategy.class=org.apache.hudi.client.clustering.plan.strategy.SparkConsistentBucketClusteringPlanStrategy");
        config.configs.add("hoodie.clustering.execution.strategy.class=org.apache.hudi.client.clustering.run.strategy.SparkConsistentBucketClusteringExecutionStrategy");
        JavaSparkContext jsc =
                UtilHelpers.buildSparkContext(ClusterJobWithConsistentBucket.class.getName() + "-hoodie", "local[*]");
        HoodieClusteringJob clusterClusteringJob = new HoodieClusteringJob(jsc, config);
//        while (true) {
            clusterClusteringJob.cluster(config.retry);
//            Thread.sleep(1000);
//        }
    }
}
