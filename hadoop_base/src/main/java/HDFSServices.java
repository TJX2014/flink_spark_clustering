import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;

public class HDFSServices {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        String[] argv = new String[]{"-format"};
        new NameNode(conf).createNameNode(argv, conf);
    }
}
