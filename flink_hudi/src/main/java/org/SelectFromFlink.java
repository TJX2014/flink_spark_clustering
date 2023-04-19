package org;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

import java.util.concurrent.ExecutionException;

public class SelectFromFlink {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration configuration = new Configuration();
//        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMinutes(1));
        configuration.set(RestOptions.PORT, 8081);
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 3);
        TableEnvironment tableEnv = TableEnvironmentImpl.create(configuration);
//        tableEnv.createFunction("listagg_order", ListAggOrderUtil.ListAgg_Order.class);
//        tableEnv.sqlQuery("add jar 'C://Users//Allen//Workspace//flink_udf_test1//target//flink_udf_test1-1.0-SNAPSHOT.jar'");
//        tableEnv.sqlUpdate("create function listagg_order 'com.deepexi.flink.ListAggOrder'");
//        tableEnv.executeSql("create catalog my_catalog with (" +
//                "'type'='hudi','mode'='hms')");
        tableEnv.executeSql("create catalog my_catalog with (" +
                "'type'='hudi','mode'='hms')");
        tableEnv.executeSql("create catalog hive_catalog with (" +
                "'type'='hive')");
        tableEnv.useCatalog("my_catalog");
//        tableEnv.useCatalog("hive_catalog");
        tableEnv.useDatabase("clustering3");
//        tableEnv.executeSql("create database if not exists my_catalog.`clustering3`");
//        tableEnv.executeSql("drop table t1");
//
//        String hoodieTableDDL = "create table if not exists t1(\n"
//                + "  uuid varchar(20),\n"
//                + "  name varchar(10),\n"
//                + "  age int,\n"
//                + "  `partition` varchar(20),\n" // test streaming read with partition field in the middle
//                + "  ts timestamp(3),\n"
//                + "  PRIMARY KEY(uuid) NOT ENFORCED\n"
//                + ")\n"
//                + "PARTITIONED BY (`partition`)\n"
//                + "with (\n"
//                + "  'connector' = 'hudi',\n" +
//                "  'table.type' = 'MERGE_ON_READ',\n" +
//                "  'hoodie.metadata.enable' = 'false'\n"
//                + ")";
//        tableEnv.executeSql(hoodieTableDDL);

//        String insertInto = "insert into t1 values\n"
//                + "('id1','Danny',23,'par1',TIMESTAMP '1970-01-01 00:00:01'),\n"
//                + "('id2','Stephen',33,'par1',TIMESTAMP '1970-01-01 00:00:02'),\n"
//                + "('id3','Julian',53,'par1',TIMESTAMP '1970-01-01 00:00:03'),\n"
//                + "('id4','Fabian',31,'par1',TIMESTAMP '1970-01-01 00:00:04')\n";
//        tableEnv.executeSql(insertInto).await();

//        String insert2Into = "insert into t1 values\n"
//                + "('id5','Sophia',18,'par1',TIMESTAMP '1970-01-01 00:00:05'),\n"
//                + "('id6','Emma',20,'par1',TIMESTAMP '1970-01-01 00:00:06'),\n"
//                + "('id7','Bob',44,'par1',TIMESTAMP '1970-01-01 00:00:07'),\n"
//                + "('id8','Han',56,'par1',TIMESTAMP '1970-01-01 00:00:08')";
//        tableEnv.executeSql(insert2Into).await();
        tableEnv.useDatabase("clustering_append");

//        String insert3Into = "insert into t1 values\n"
//                + "('id9','xiaoxingstack',18,'par1',TIMESTAMP '1970-01-01 00:00:05')\n";
//        tableEnv.executeSql(insert3Into).await();

        tableEnv.executeSql("select * from t1").print();

//        CloseableIterator<Row> result = tableEnv.executeSql("select * from t1").collect();
//        List<Row> rows = new ArrayList<>();
//        while (result.hasNext()) {
//            rows.add(result.next());
//        }
//        System.out.println(rows);

//        tableEnv.executeSql(insert2Into);

    }
}
