package udf;

import com.deepexi.flink.function.JsonPlant;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ToHudiJson {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMinutes(1));
        configuration.set(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofMinutes(1));
        configuration.set(RestOptions.PORT, 8081);
        configuration.set(RestOptions.ENABLE_FLAMEGRAPH, true);
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 2);
        TableEnvironment tableEnv = TableEnvironmentImpl.create(configuration);
        tableEnv.executeSql("create catalog my_catalog with (" +
                "'type'='hudi','mode'='hms')");
        tableEnv.createFunction("jsonplant", JsonPlant.class);
        tableEnv.executeSql("create database if not exists my_catalog.`debug`");
//        tableEnv.executeSql("use catalog my_catalog");
//        tableEnv.executeSql("use debug");
//        tableEnv.executeSql("drop table json_test2");
        tableEnv.executeSql("CREATE TABLE if not exists my_catalog.`debug`.json_test2(\n" +
                "  uuid string,\n" +
                "  subtext string\n" +
                ")\n" +
                "WITH (\n" +
                "  'connector' = 'hudi',\n" +
                "  'table.type' = 'COPY_ON_WRITE',\n" +
                "  'hoodie.metadata.enable' = 'false',\n" +
                "  'hive_sync.enabled' = 'true',\n" +
                "  'hive_sync.db' = 'debug',\n" +
                "  'hive_sync.table' = 't2',\n" +
                "  'hive_sync.metastore.uris' = 'thrift://localhost:9099',\n" +
                "  'hoodie.datasource.write.hive_style_partitioning' = 'true'\n" +
//                "  ,'index.type' = 'BUCKET'\n" +
                ")");
//        tableEnv.executeSql("insert into my_catalog.`debug`.json_test2 VALUES ('2', '{\"META_DATA\":[{\"code\":\"CP\",\"name\":\"毛管压力\"},{\"code\":\"DRIVE_PRECESS\",\"name\":\"驱动过程\"},{\"code\":\"SHG\",\"name\":\"汞饱和度\"}],\"DATA\":[{\"SHG\":0.81,\"DRIVE_PRECESS\":\"进汞\",\"CP\":0.025},{\"SHG\":86.74,\"DRIVE_PRECESS\":\"退汞\",\"CP\":15.1}]}')");
//        tableEnv.executeSql("select * from json_test2");
        CloseableIterator<Row> result = tableEnv.executeSql("select item['DRIVE_PRECESS'],item['CP'],item['SHG'],my_catalog.debug.json_test2.subtext " +
                "from my_catalog.debug.json_test2,lateral table(jsonplant(subtext)) as T(item) ").collect();
        List<Row> rows = new ArrayList<>();
        while (result.hasNext()) {
           rows.add(result.next());
        }
        System.out.println(rows);
    }
}
