package org;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

import java.time.Duration;

public class MVCCToHudi {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
//        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMinutes(1));
        Duration checkpointInterval = Duration.ofSeconds(10);
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, checkpointInterval);
        configuration.set(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, checkpointInterval);
        configuration.set(RestOptions.PORT, 8081);
        configuration.set(RestOptions.ENABLE_FLAMEGRAPH, true);
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        TableEnvironment tableEnv = TableEnvironmentImpl.create(configuration);

        tableEnv.executeSql("create catalog my_catalog with (" +
                "'type'='hudi','mode'='hms')");
//        tableEnv.executeSql("drop table my_catalog.`clustering3`.t2");
//        tableEnv.executeSql("create database if not exists my_catalog.`clustering3`");
        tableEnv.executeSql("CREATE TABLE if not exists my_catalog.`clustering3`.t2(\n" +
                "  uuid bigint primary key,\n" +
                "  name VARCHAR(10),\n" +
                "  age INT,\n" +
                "  ts TIMESTAMP(6),\n" +
                "  `partition` VARCHAR(20)\n" +
                ")\n" +
                "PARTITIONED BY (`partition`)\n" +
                "WITH (\n" +
                "  'connector' = 'hudi',\n" +
                "  'table.type' = 'MERGE_ON_READ',\n" +
                "  'hoodie.metadata.enable' = 'false',\n" +
                "  'hive_sync.enabled' = 'true',\n" +
                "  'hive_sync.db' = 'clustering3',\n" +
                "  'hive_sync.table' = 't2',\n" +
                "  'hive_sync.metastore.uris' = 'thrift://localhost:9099',\n" +
                "  'hoodie.datasource.write.hive_style_partitioning' = 'true'\n" +
//                "  ,'index.type' = 'BUCKET'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE t1_print(\n" +
                "  uuid bigint PRIMARY KEY NOT ENFORCED,\n" +
                "  name VARCHAR(10),\n" +
                "  age INT,\n" +
                "  ts TIMESTAMP(3)\n" +
                ")\n" +
                "WITH (\n" +
                  "  'connector' = 'print'" +
//                "  'connector' = 'filesystem',\n" +
//                "  'format' = 'csv',\n" +
//                "  'path' = 't1_print/'\n" +
                ")");

        tableEnv.executeSql("create table t1_src(\n" +
                "uuid bigint,\n" +
                "name string,\n" +
                "age int,\n" +
                "ts timestamp(3)\n" +
                ") with (\n" +
                " 'connector'='datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.uuid.start'='1',\n" +
                " 'fields.uuid.end'='60',\n" +
                " 'fields.uuid.kind'='sequence',\n" +
                " 'fields.name.length'='6',\n" +
                " 'fields.age.kind'='random',\n" +
                "'fields.age.min'='10',\n" +
                "'fields.age.max'='100',\n" +
                "'fields.ts.kind'='random'\n" +
                ")");

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql("insert into " +
                "my_catalog.`clustering3`.t2 " +
                "/*+OPTIONS('hive_sync.enabled' = 'false'," +
                "'hoodie.write.concurrency.mode' = 'optimistic_concurrency_control'," +
                "'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider'," +
                "'hoodie.compact.inline.max.delta.commits' = '3'," +
                "'compaction.async.enabled' = 'true'" +
                ") */" +
                " select uuid, name, age, ts, '20230405' from t1_src");
        statementSet.addInsertSql("insert into t1_print select * from t1_src");

        statementSet.execute();
    }
}
