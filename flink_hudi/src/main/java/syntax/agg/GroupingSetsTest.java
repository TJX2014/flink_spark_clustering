package syntax.agg;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

import java.time.Duration;

public class GroupingSetsTest {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
//        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMinutes(1));
        Duration checkpointInterval = Duration.ofSeconds(10);
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, checkpointInterval);
        configuration.set(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, checkpointInterval);
        configuration.set(RestOptions.PORT, 8081);

        TableEnvironment tableEnv = TableEnvironmentImpl.create(configuration);
        tableEnv.executeSql("CREATE TABLE scott_emp (\n" +
                " empno int,\n" +
                " ename varchar,\n" +
                " job VARCHAR,\n" +
                " mgr int,\n" +
                " hiredate timestamp,\n" +
                " sal double,\n" +
                " comm double,\n" +
                " deptno int\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE sink (\n" +
                " deptno int,\n" +
                " job VARCHAR,\n" +
                " c bigint,\n" +
                " d bigint,\n" +
                " j bigint,\n" +
                " x bigint\n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")");

        Configuration conf = tableEnv.getConfig().getConfiguration();
//        conf.setString("table.exec.mini-batch.enabled", "TRUE");
//        conf.setString("table.exec.mini-batch.allow-latency", "5 s");
//        conf.setString("table.exec.mini-batch.size", "5");
        tableEnv.executeSql("insert into sink SELECT " +
                "    deptno,\n" +
                "    job,\n" +
                "    COUNT(*) AS c,\n" +
                "    GROUPING(deptno) AS d,\n" +
                "    GROUPING(job) j,\n" +
                "    GROUPING(deptno, job) AS x\n" +
                "FROM scott_emp GROUP BY CUBE(deptno, job)");

        tableEnv.execute("GroupingSetsTest");
    }
}
