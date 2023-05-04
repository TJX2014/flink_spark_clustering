package syntax.join;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

public class LookupJoinTest {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        TableEnvironment tableEnv = TableEnvironmentImpl.create(configuration);
        tableEnv.executeSql("CREATE TABLE MyTable (\n" +
                " a int,\n" +
                " b varchar,\n" +
                " c bigint,\n" +
                "  proctime as PROCTIME(),\n" +
                "  rowtime TIMESTAMP(3)\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE LookupTable (\n" +
                " id int primary key,\n" +
                " name varchar,\n" +
                " age int\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE sink (\n" +
                " a int,\n" +
                " name varchar,\n" +
                " age int\n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")");

        Configuration conf = tableEnv.getConfig().getConfiguration();

        tableEnv.executeSql("" +
                "SELECT * FROM MyTable AS T LEFT JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id");
//        tableEnv.executeSql("insert into sink \n" +
//                "        SELECT \n" +
//                "// /*+ LOOKUP('table'='D', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s', 'max-attempts'='3') */ \n" +
//                "        T.a, D.name, D.age \n" +
//                "        FROM MyTable T \n" +
//                "        JOIN LookupTable\n" +
//                "        FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id \n" +
//                "");
        tableEnv.execute("LookupJoinTest");
    }
}
