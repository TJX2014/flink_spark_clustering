package syntax.agg;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

import java.time.Duration;

public class TwoStageAggregateTest {

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
                " b bigint,\n" +
                " c string\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE sink (\n" +
                "min_c varchar, \n" +
                "avg_a int \n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")");

        Configuration conf = tableEnv.getConfig().getConfiguration();
        conf.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
        conf.setString("table.exec.mini-batch.enabled", "TRUE");
        conf.setString("table.exec.mini-batch.allow-latency", "5 s");
        conf.setString("table.exec.mini-batch.size", "5");
//        conf.setString("table.exec.mini-batch.allow-latency", "5 s");
//        conf.setString("table.exec.mini-batch.size", "5");

        tableEnv.executeSql("insert into sink SELECT MIN(c), AVG(a) " +
                "FROM (SELECT a, b + 3 AS d, c FROM MyTable) GROUP BY d");

        tableEnv.execute("TwoStageAggregateTest");
    }
}
