package syntax.agg;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

import java.time.Duration;

public class GroupWindowTest {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
//        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMinutes(1));
        Duration checkpointInterval = Duration.ofSeconds(10);
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, checkpointInterval);
        configuration.set(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, checkpointInterval);
        configuration.set(RestOptions.PORT, 8081);

        TableEnvironment tableEnv = TableEnvironmentImpl.create(configuration);
        tableEnv.executeSql("CREATE TABLE MyTable1 (\n" +
                " a int,\n" +
                " b bigint,\n" +
                " c as proctime()\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE sink (\n" +
                "sum_a bigint, sum_b bigint" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")");

        Configuration conf = tableEnv.getConfig().getConfiguration();
//        conf.setString("table.exec.mini-batch.enabled", "TRUE");
//        conf.setString("table.exec.mini-batch.allow-latency", "5 s");
//        conf.setString("table.exec.mini-batch.size", "5");
        tableEnv.executeSql("insert into sink select sum(a), max(b)\n" +
                "from MyTable1\n" +
                "group by HOP(c, INTERVAL '1' SECOND, INTERVAL '1' MINUTE)");

        tableEnv.execute("GroupingSetsTest");
    }
}
