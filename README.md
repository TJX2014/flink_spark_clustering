
clone hudi项目：https://github.com/apache/hudi
安装整个hudi:
mvn clean install -Dflink1.14 -Dspark2.4 -Dscala-2.12 -DskipTests

mvn clean install -Dflink1.14 -Dspark3.2 -Dscala-2.12 -DskipTests

安装本地flink最新版本：
 mvn clean install -Dflink1.14 -Dspark2.4 -Dscala-2.12 -DskipTests -D"rat.skip=true" -pl packaging/hudi-flink-bundle -am

安装spark
2.4:
mvn clean install -Dflink1.14 -Dspark2.4 -Dscala-2.12 -DskipTests -pl packaging/hudi-spark-bundle
3.2:
mvn clean install -Dflink1.14 -Dspark3.2 -Dscala-2.12 -DskipTests -pl packaging/hudi-spark-bundle -am

安装utilities
2.4:
mvn clean install -Dflink1.14 -Dspark2.4 -Dscala-2.12 -DskipTests -pl hudi-utilities
3.2:
mvn clean install -Dflink1.14 -Dspark3.2 -Dscala-2.12 -DskipTests -pl hudi-utilities -am

flink打包
mvn package -DskipTests -D"rat.skip=true" -D"spotless.check.skip=true" -D"checkstyle.skip=true" -P"scala-2.12"

windows上运行hadoop客户端入hudi需要把winutils.exe放到HADOOP_HOME/bin下
winutils下载地址为：https://github.com/cdarlint/winutils

