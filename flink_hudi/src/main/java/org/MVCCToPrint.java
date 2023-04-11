package org;

import com.deepexi.flink.ListAggOrderUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

public class MVCCToPrint {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMinutes(1));
        configuration.set(RestOptions.PORT, 8081);
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 3);
        TableEnvironment tableEnv = TableEnvironmentImpl.create(configuration);
        tableEnv.createFunction("listagg_order", ListAggOrderUtil.ListAgg_Order.class);
//        tableEnv.sqlQuery("add jar 'C://Users//Allen//Workspace//flink_udf_test1//target//flink_udf_test1-1.0-SNAPSHOT.jar'");
//        tableEnv.sqlUpdate("create function listagg_order 'com.deepexi.flink.ListAggOrder'");
//        tableEnv.executeSql("create catalog my_catalog with (" +
//                "'type'='hudi','mode'='hms')");
//        tableEnv.executeSql("create database if not exists my_catalog.`clustering3`");
//        tableEnv.executeSql("CREATE TABLE if not exists my_catalog.`clustering3`.t3(\n" +
//                "  uuid bigint primary key,\n" +
//                "  name VARCHAR(10),\n" +
//                "  age INT,\n" +
//                "  ts TIMESTAMP(6),\n" +
//                "  `partition` VARCHAR(20)\n" +
//                ")\n" +
//                "PARTITIONED BY (`partition`)\n" +
//                "WITH (\n" +
//                "  'connector' = 'hudi',\n" +
//                "  'table.type' = 'MERGE_ON_READ',\n" +
//                "  'hoodie.metadata.enable' = 'false',\n" +
//                "  'hive_sync.enabled' = 'true',\n" +
//                "  'hive_sync.db' = 'clustering3',\n" +
//                "  'hive_sync.table' = 't3',\n" +
//                "  'hive_sync.metastore.uris' = 'thrift://localhost:9099',\n" +
//                "  'hoodie.datasource.write.hive_style_partitioning' = 'true'\n" +
////                "  ,'index.type' = 'BUCKET'\n" +
//                ")");
        tableEnv.executeSql("CREATE TABLE t1(\n" +
                "  uuid bigint,\n" +
                "  name string\n" +
                ")\n" +
                "WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");
        tableEnv.executeSql("create table t1_src(\n" +
                "uuid bigint,\n" +
                "name string,\n" +
                "age int,\n" +
                "ts timestamp(3)\n" +
                ") with (\n" +
                " 'connector'='datagen',\n" +
                " 'rows-per-second'='100',\n" +
                " 'fields.uuid.min'='0',\n" +
                " 'fields.uuid.max'='1000',\n" +
                " 'fields.uuid.kind'='random',\n" +
                " 'fields.name.length'='6',\n" +
                " 'fields.age.kind'='random',\n" +
                "'fields.age.min'='10',\n" +
                "'fields.age.max'='100',\n" +
                "'fields.ts.kind'='random'\n" +
                ")");
        tableEnv.executeSql("insert into t1 " +
                " select uuid, listagg_order(name, ';', false) from t1_src group by uuid").await();
    }
}
