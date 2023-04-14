package syntax;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.planner.parse.CalciteParser;

import static org.apache.calcite.avatica.util.Quoting.BACK_TICK;

public class TestSyntax {

    public static void main(String[] args) {
        SqlParser.Config config1 = SqlParser.Config.DEFAULT.withCaseSensitive(false)
                .withQuotedCasing(Casing.UNCHANGED)
                .withLex(Lex.MYSQL)
                .withParserFactory(FlinkSqlParserImpl.FACTORY)
                .withQuoting(BACK_TICK)
                .withConformance(FlinkSqlConformance.DEFAULT);
        CalciteParser parser1 = new CalciteParser(config1);
        SqlParser.Config config2 = SqlParser.Config.DEFAULT.withCaseSensitive(false)
                .withQuotedCasing(Casing.UNCHANGED)
                .withLex(Lex.JAVA)
                .withParserFactory(FlinkSqlParserImpl.FACTORY)
                .withQuoting(BACK_TICK)
                .withConformance(FlinkSqlConformance.DEFAULT);
        CalciteParser parser2 = new CalciteParser(config2);
        String sql = "select item['DRIVE_PRECESS'],item['CP'],item['SHG'] from json_test2,lateral table(jsonplant(`subtext`)) as T(item)";
        SqlNode result1 = parser1.parse(sql);
        SqlNode result2 = parser2.parse(sql);
    }
}
