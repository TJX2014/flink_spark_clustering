import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

import java.io.IOException;

public class HDFSServices {
    public static void main(String[] args) throws Exception {
//        new MiniDFSCluster().startDataNodes(new Configuration());
        Configuration conf = new Configuration();
//        String[] argv = new String[]{"-format"};
        String[] argv = new String[]{};
//        NameNode.main(argv);
//        new NameNode(conf)
        DefaultMetricsSystem.setMiniClusterMode(true);
        new NameNode(conf).createNameNode(argv, conf).join();
    }
}
