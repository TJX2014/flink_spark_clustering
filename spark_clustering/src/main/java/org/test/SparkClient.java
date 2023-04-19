package org.test;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.spark.deploy.SparkSubmit$;
import org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver;

import java.util.Map;

public class SparkClient {

    public static void main(String[] args) throws Exception {
//        builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        builder.config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar");
//        builder.config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
//        builder.config("spark.kryoserializer.buffer.max", "512m");

        String[] cmd = new String[] {"--class", "org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver",
            "--master", "local[*]", "."};
        SparkSubmit$.MODULE$.main(cmd);

//        HiveConf conf = new HiveConf();
//        System.setProperty("spark.master", "local[*]");
//        SessionState.setCurrentSessionState(new CliSessionState(conf));
//        SparkSQLCLIDriver sparkSQLCLIDriver = new SparkSQLCLIDriver();
//        sparkSQLCLIDriver.run(args);

    }
}
