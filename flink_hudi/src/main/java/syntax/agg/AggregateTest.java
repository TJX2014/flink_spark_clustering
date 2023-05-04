package syntax.agg;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

public class AggregateTest {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        TableEnvironment tableEnv = TableEnvironmentImpl.create(configuration);
        tableEnv.executeSql("CREATE TABLE src (\n" +
                " a VARCHAR,\n" +
                " b int,\n" +
                " c VARCHAR,\n" +
                " d boolean\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE src2 (\n" +
                " a VARCHAR,\n" +
                " b int,\n" +
                " c VARCHAR,\n" +
                " d boolean\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE sink (\n" +
                " a VARCHAR,\n" +
                " sum_b bigint,\n" +
                " count_c1 bigint,\n" +
                " count_c2 bigint,\n" +
                " max_b bigint\n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")");

//        tableEnv.executeSql("insert into sink SELECT\n" +
//                "  a,\n" +
//                "  SUM(b) FILTER (WHERE c = 'A'),\n" +
//                "  COUNT(DISTINCT c) FILTER (WHERE d is true),\n" +
//                "  COUNT(DISTINCT c) FILTER (WHERE b = 1),\n" +
//                "  MAX(b)\n" +
//                "FROM src GROUP BY a");
//        tableEnv.sqlUpdate("set 'table.exec.mini-batch.enabled'='true'");
//        tableEnv.sqlUpdate("set 'table.exec.mini-batch.allow-latency'='5 s'");
        Configuration conf = tableEnv.getConfig().getConfiguration();
        conf.setString("table.exec.mini-batch.enabled", "TRUE");
//        conf.setString("table.exec.mini-batch.enabled", "true");
        conf.setString("table.exec.mini-batch.allow-latency", "5 s");
        conf.setString("table.exec.mini-batch.size", "5");
        tableEnv.executeSql("insert into sink SELECT a, SUM(b), COUNT(DISTINCT c), " +
                "cast(0 as bigint), cast(0 as bigint)\n" +
                "FROM (\n" +
                "  SELECT * FROM src\n" +
                "  UNION ALL\n" +
                "  SELECT * FROM src2\n" +
                ") GROUP BY a");

        tableEnv.execute("AggregateTest");
    }
}
