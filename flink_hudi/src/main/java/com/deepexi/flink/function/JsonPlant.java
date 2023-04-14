package com.deepexi.flink.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@FunctionHint(output = @DataTypeHint("map<string, string>"))
public class JsonPlant extends TableFunction<Map> {

    private static Logger LOG = LoggerFactory.getLogger(JsonPlant.class);

    private static ObjectMapper objectMapper = new ObjectMapper();

    public void eval(String json) throws IOException {
        for (Map item: plantJson(json)) {
            collect(item);
        }
    }

    private static List<Map> plantJson(String content) throws IOException {
        Object o = objectMapper.readValue(content, Object.class);
        if (o instanceof HashMap) {
            Map total = (HashMap)o;
            List<Map> metas = (List) total.get("META_DATA");
            List<String> items = new ArrayList<>(metas.size());
            for (Map kv: metas) {
                items.add((String) kv.get("code"));
            }
            List<Map> data = (List) total.get("DATA");
            List<Map> resultRows = new ArrayList<>(data.size());
            for (Map rowData: data) {
                Map row = new HashMap(items.size());
                for (int i=0; i<items.size(); i++) {
                    row.put(String.valueOf(items.get(i)), String.valueOf(rowData.get(items.get(i))));
                }
                resultRows.add(row);
            }
            return resultRows;
        } else {
            LOG.warn("content:'" + content + "'is not hashmap");
        }
        return null;
    }

    public static void main(String[] args) throws IOException {
        String content = "{\"META_DATA\":[{\"code\":\"CP\",\"name\":\"毛管压力\"},{\"code\":\"DRIVE_PRECESS\",\"name\":\"驱动过程\"},{\"code\":\"SHG\",\"name\":\"汞饱和度\"}],\"DATA\":[{\"SHG\":0.81,\"DRIVE_PRECESS\":\"进汞\",\"CP\":0.025},{\"SHG\":86.74,\"DRIVE_PRECESS\":\"退汞\",\"CP\":15.1}]}";
        String content2 = "{\"META_DATA\":[{\"code\":\"CP\",\"name\":\"毛管压力\"},{\"code\":\"DRIVE_PRECESS\",\"name\":\"驱动过程\"},{\"code\":\"SHG\",\"name\":\"汞饱和度\"},{\"code\":\"SHG1\",\"name\":\"汞饱和度11\"}],\"DATA\":[{\"SHG\":0.81,\"SHG1\":0.82,\"DRIVE_PRECESS\":\"进汞\",\"CP\":0.025},{\"SHG\":86.74,\"DRIVE_PRECESS\":\"退汞\",\"CP\":15.1}]}";
        List<Map> results = plantJson(content);
        List<Map> result2 = plantJson(content2);
        System.out.println(results);
    }
}
