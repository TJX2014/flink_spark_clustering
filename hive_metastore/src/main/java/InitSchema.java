public class InitSchema {

    public static void main(String[] args) {
        org.apache.hive.beeline.HiveSchemaTool
                .main(new String[]{"-initSchema", "-dbType", "mysql"});
    }
}
