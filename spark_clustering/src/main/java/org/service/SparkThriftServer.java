package org.service;

import org.apache.spark.deploy.SparkSubmit$;

public class SparkThriftServer {

    public static void main(String[] args) throws Exception {
//        builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        builder.config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar");
//        builder.config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
//        builder.config("spark.kryoserializer.buffer.max", "512m");

//        String[] cmd = new String[] {"--class", "org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver",
//            "--master", "local[*]", "."};
        String[] cmd = new String[] {
                "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
                "--conf", "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
                "--class", "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
                "--master", "local[*]", "."};
        SparkSubmit$.MODULE$.main(cmd);
    }
}
