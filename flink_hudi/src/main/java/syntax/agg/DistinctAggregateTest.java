package syntax.agg;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

public class DistinctAggregateTest {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        TableEnvironment tableEnv = TableEnvironmentImpl.create(configuration);
        tableEnv.executeSql("CREATE TABLE src (\n" +
                " a VARCHAR,\n" +
                " b varchar,\n" +
                " c VARCHAR,\n" +
                " d boolean\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE sink (\n" +
                " b bigint,\n" +
                " first_c VARCHAR,\n" +
                " last_c VARCHAR,\n" +
                " count_c bigint\n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")");

        Configuration conf = tableEnv.getConfig().getConfiguration();
//        conf.setString("table.exec.mini-batch.enabled", "TRUE");
//        conf.setString("table.exec.mini-batch.allow-latency", "5 s");
//        conf.setString("table.exec.mini-batch.size", "5");
        conf.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
        conf.setString("table.optimizer.distinct-agg.split.enabled", "true");
        tableEnv.executeSql("insert into sink SELECT\n" +
                "  b, FIRST_VALUE(c), LAST_VALUE(c), COUNT(DISTINCT c)\n" +
                "FROM(\n" +
                "  SELECT\n" +
                "    a, COUNT(DISTINCT b) as b, MAX(b) as c\n" +
                "  FROM src\n" +
                "  GROUP BY a\n" +
                ") GROUP BY b");

        tableEnv.execute("DistinctAggregateTest");
    }
}
