package flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

public class ToHudi {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMinutes(1));
        configuration.set(RestOptions.PORT, 8081);
        TableEnvironment tableEnv = TableEnvironmentImpl.create(configuration);
        tableEnv.executeSql("CREATE TABLE t1(\n" +
                "  uuid VARCHAR(20) PRIMARY KEY NOT ENFORCED,\n" +
                "  name VARCHAR(10),\n" +
                "  age INT,\n" +
                "  ts TIMESTAMP(3),\n" +
                "  `partition` VARCHAR(20)\n" +
                ")\n" +
                "PARTITIONED BY (`partition`)\n" +
                "WITH (\n" +
                "  'connector' = 'hudi',\n" +
                "  'path' = 'D://data//hudi/t1',\n" +
                "  'table.type' = 'MERGE_ON_READ',\n" +
                "  'index.type' = 'BUCKET'\n" +
                ")");
//        tableEnv.executeSql("CREATE TABLE t1(\n" +
//                "  uuid VARCHAR(20) PRIMARY KEY NOT ENFORCED,\n" +
//                "  name VARCHAR(10),\n" +
//                "  age INT,\n" +
//                "  ts TIMESTAMP(3),\n" +
//                "  `partition` VARCHAR(20)\n" +
//                ")\n" +
//                "WITH (\n" +
//                "  'connector' = 'print'\n" +
//                ")");
        tableEnv.executeSql("create table t1_src(\n" +
                "uuid string,\n" +
                "name string,\n" +
                "age int,\n" +
                "ts timestamp(3)\n" +
                ") with (\n" +
                " 'connector'='datagen',\n" +
                " 'rows-per-second'='5',\n" +
                " 'fields.uuid.length'='6',\n" +
                " 'fields.name.length'='6',\n" +
                " 'fields.age.kind'='random',\n" +
                "'fields.age.min'='10',\n" +
                "'fields.age.max'='100',\n" +
                "'fields.ts.kind'='random'\n" +
                ")");
        tableEnv.executeSql("insert into t1 select uuid, name, age, ts, '20230405' from t1_src").await();
    }
}
