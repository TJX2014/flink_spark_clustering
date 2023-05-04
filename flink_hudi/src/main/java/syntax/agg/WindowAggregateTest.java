package syntax.agg;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

import java.time.Duration;

public class WindowAggregateTest {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
//        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMinutes(1));
        Duration checkpointInterval = Duration.ofSeconds(10);
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, checkpointInterval);
        configuration.set(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, checkpointInterval);
        configuration.set(RestOptions.PORT, 8081);

        TableEnvironment tableEnv = TableEnvironmentImpl.create(configuration);
        tableEnv.executeSql("\n" +
                "CREATE TABLE MyTable (\n" +
                "  a INT,\n" +
                "  b BIGINT,\n" +
                "  c STRING NOT NULL,\n" +
                "  d DECIMAL(10, 3),\n" +
                "  e BIGINT,\n" +
                "  rowtime TIMESTAMP(3),\n" +
                "  proctime as PROCTIME(),\n" +
                "  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND\n" +
                ") with (\n" +
                "  'connector' = 'datagen'\n" +
                ")\n");

        tableEnv.executeSql("CREATE TABLE sink (\n" +
                "a int, \n" +
                "b bigint" +
                ", \n" +
                "row_num bigint , \n" +
                "max_d DECIMAL(10, 3)\n" +
//                ",uv bigint \n" +
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

        tableEnv.executeSql("insert into sink SELECT\n" +
                "   a,\n" +
                "   b \n" +
                ",\n" +
                "   count(*) \n" +
                ",\n" +
                "   max(d) filter (where b > 1000) \n" +
                "//,count(distinct window_time) AS uv\n" +
                "FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))\n" +
                "GROUP BY window_start, a, window_end, b");

//        tableEnv.execute("WindowAggregateTest");
    }
}
