package syntax.join;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

public class IntervalJoinTest {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        TableEnvironment tableEnv = TableEnvironmentImpl.create(configuration);
        tableEnv.executeSql("CREATE TABLE MyTable (\n" +
                " a int,\n" +
                " b varchar,\n" +
                " c bigint,\n" +
                " d boolean,\n" +
                "  proctime as PROCTIME(),\n" +
                "  rowtime TIMESTAMP(3)\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE MyTable2 (\n" +
                " a int,\n" +
                " b varchar,\n" +
                " c bigint,\n" +
                " d boolean,\n" +
                "  proctime as PROCTIME(),\n" +
                "  rowtime TIMESTAMP(3)\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE sink (\n" +
                " a int,\n" +
                " c bigint,\n" +
                " c0 bigint\n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")");

        Configuration conf = tableEnv.getConfig().getConfiguration();

        tableEnv.executeSql("insert into sink WITH T1 AS (SELECT a, b, c, proctime, CAST(null AS BIGINT) AS nullField FROM MyTable),\n" +
                "        T2 AS (SELECT a, b, c, proctime, CAST(null AS BIGINT) AS nullField FROM MyTable2) \n" +
                "        SELECT t2.a, t2.c, t1.c\n" +
                "        FROM T1 AS t1\n" +
                "        JOIN T2 AS t2 ON t1.a = t2.a \n" +
//                "           AND t1.nullField = t2.nullField \n" +
                "          AND t1.proctime BETWEEN t2.proctime - INTERVAL '5' SECOND AND\n" +
                "          t2.proctime + INTERVAL '5' SECOND" +
                "");
        tableEnv.execute("IntervalJoinTest");
    }
}
