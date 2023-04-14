package org.example;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class CRUDTable2 {

    public static void main(String[] args) throws AnalysisException, ClassNotFoundException {
        SparkSession.Builder builder = SparkSession.builder().appName("hudi_crud");
        builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        builder.config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar");
        builder.config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
        builder.config("spark.kryoserializer.buffer.max", "512m");
//        builder.config("spark.sql.warehouse.dir", WAREHOUSE);
        builder.master("local[*]");
        builder.enableHiveSupport();
        SparkSession spark = builder.getOrCreate();
//        spark.read().format("hudi").load(TABLE_PATH_TABLE1).createTempView("t1");
        List<Row> dbs = spark.sql("show databases").collectAsList();
        spark.sql("set hoodie.metadata.enable=false");
        spark.sql("set hoodie.write.concurrency.mode=optimistic_concurrency_control");
        spark.sql("set hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider");
//        spark.sessionState()
//        spark.sql("create database test2");
//        spark.sql("show databases").collectAsList();
//        spark.sql(
//                "CREATE TABLE if not exists t1(\n" +
//                        "  uuid bigint,\n" +
//                        "  name string,\n" +
//                        "  age INT,\n" +
//                        "  ts TIMESTAMP,\n" +
//                        "  `partition` string\n" +
//                        ")\n" +
//                        "using hudi \n" +
//                        "location '" + TABLE_PATH_TABLE1 + "'\n" +
//                        "options (\n" +
//                        "  'primaryKey' = 'uuid',\n" +
//                        "  'preCombineField' = 'ts',\n" +
//                        "  'type' = 'MERGE_ON_READ'\n" +
//                        ")\n" +
//                        "PARTITIONED BY (`partition`)");

//        spark.sql("insert into t1 select 100,'aaa',75,timestamp(16807831), '20230405'");
        spark.sql("use clustering");
        System.out.println("update once");
//        spark.sql("update clustering3.t3 set name='xiaoxing111' where uuid in (100, 101, 102)");
        List<Row> rows = spark.sql("select * from clustering3.t2").collectAsList();
        System.out.println("result rows:" + rows);
//        while (true) {
//            try {
//                System.out.println("update once");
//                spark.sql("update clustering3.t3 set name='xiaoxing111' where uuid in (100, 101, 102)");
//                rows = spark.sql("select * from clustering3.t3 where uuid in (100, 101, 102)").collectAsList();
//                System.out.println("result rows:" + rows);
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                break;
//            }
//        }
        spark.sql("update t1_ro set name='xiaoxing111' where uuid in (100, 101, 102)");
        List<Row> result = spark.sql("select * from t1 where uuid in (100, 101, 102)").collectAsList();
        spark.sql("insert into t1_ro select 100,'aaa',75,timestamp(16807831), '20230405'");
        System.out.println(result);
    }
}
