package org.example;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.example.ClusterJob.TABLE_PATH_TABLE1;

public class ReadTable {

    public static void main(String[] args) throws AnalysisException {
        SparkSession.Builder builder = SparkSession.builder().appName("hudi_read");
        builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        builder.config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar");
        builder.config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
        builder.config("spark.kryoserializer.buffer.max", "512m");
        builder.master("local[*]");
        SparkSession spark = builder.getOrCreate();
        spark.read().format("hudi").load(TABLE_PATH_TABLE1).createTempView("t1");
        List<Row> result = spark.sql("select * from t1").collectAsList();
        System.out.println(result);
    }
}
