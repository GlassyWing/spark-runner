package org.manlier.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Create by manlier 2018/8/13 9:32
 */
public class JsonUtil {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static Map<String, Object> parseToMap(String jsonStr) throws IOException {
        System.out.println(mapper.valueToTree(jsonStr));

        return mapper.readValue(jsonStr, new TypeReference<Map<String, Object>>() {
        });
    }

    public static void main(String[] args) throws IOException {
        String jsonStr = "[{\"a\":3, \"b\":4}]";
        JsonNode node = mapper.valueToTree(jsonStr);

        List<Map<String, Object>> data = mapper.readValue(node.asText(),
                new TypeReference<List<Map<String, Object>>>() {});

        System.out.println(data);

//        System.out.println(parseToMap(jsonStr));
    }
}
