package flink;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.table.HoodieTableMetaClient;

public class TestPath {
    public static void main(String[] args) {

        new Path("basePath", HoodieTableMetaClient.METAFOLDER_NAME);
    }
}
