package syntax.agg;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

import java.time.Duration;

public class OverAggregateTest {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
//        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMinutes(1));
        Duration checkpointInterval = Duration.ofSeconds(10);
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, checkpointInterval);
        configuration.set(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, checkpointInterval);
        configuration.set(RestOptions.PORT, 8081);

        TableEnvironment tableEnv = TableEnvironmentImpl.create(configuration);
        tableEnv.executeSql("CREATE TABLE MyTable (\n" +
                " a int,\n" +
                " b string,\n" +
                " c bigint,\n" +
                " proctime as proctime()\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE sink (\n" +
                "b varchar, \n" +
                "count_a bigint, \n" +
                "sum_a bigint, \n" +
                "count_distinct_a bigint, \n" +
                "sum_distinct_c bigint" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")");

        Configuration conf = tableEnv.getConfig().getConfiguration();
//        conf.setString("table.exec.mini-batch.enabled", "TRUE");
//        conf.setString("table.exec.mini-batch.allow-latency", "5 s");
//        conf.setString("table.exec.mini-batch.size", "5");
        tableEnv.executeSql("insert into sink SELECT " +
                "b,\n" +
                "    COUNT(a) OVER w AS cnt1,\n" +
                "    SUM(a) OVER w AS sum1,\n" +
                "    COUNT(DISTINCT a) OVER w AS cnt2,\n" +
                "    SUM(DISTINCT c) OVER w AS sum2\n" +
                "FROM MyTable\n" +
                "    WINDOW w AS (PARTITION BY b ORDER BY proctime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)");

        tableEnv.execute("OverAggregateTest");
    }
}
