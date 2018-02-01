package ca.uwaterloo.dsg;

import org.json.simple.JsonArray;
import org.json.simple.JsonObject;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonUtil {
    private static JsonUtil instance = null;
    protected JsonUtil() {}

    private List<JsonObject> sortBigDecimalsArray(JsonArray array, String sortKey) {
        List<JsonObject> sorted = new ArrayList<>();
        Map<BigDecimal, JsonObject> map = new HashMap<>();

        JsonObject entry;
        for (Object json : array) {
            entry = (JsonObject) json;
            map.put((BigDecimal) entry.get(sortKey), entry);
        }

        List<BigDecimal> keys = new ArrayList<>();
        keys.addAll(map.keySet());
        Collections.sort(keys);

        for (BigDecimal key: keys) {
            sorted.add(map.get(key));
        }

        return sorted;
    }

    private  List<JsonObject> sortStringJsonArray(JsonArray array, String sortKey) {
        List<JsonObject> sorted = new ArrayList<>();
        Map<String, JsonObject> map = new HashMap<>();

        JsonObject entry;
        for (Object json : array) {
            entry = (JsonObject) json;
            map.put((String) entry.get(sortKey), entry);
        }

        List<String> keys = new ArrayList<>();
        keys.addAll(map.keySet());
        Collections.sort(keys);

        for (String key: keys) {
            sorted.add(map.get(key));
        }

        return sorted;
    }

    public List<JsonObject> sortJsonArray(JsonArray array, String sortKey) {
        // json array in spark logs are not sorted based on ids (Stage ID, RDD ID etc)
        // Due to dependencies among RDD and Stages, sorting Json array is necessary
        // in order to extract necessary information with single run through

        List<JsonObject> sorted = new ArrayList<>();
        if (array.size() > 0) {
            JsonObject sampleJson = (JsonObject) array.get(0);
            Object key = sampleJson.get(sortKey);

            // key can be any type
            if (key instanceof String) {
                sorted.addAll(sortStringJsonArray(array, sortKey));
            } else if (key instanceof BigDecimal) {
                sorted.addAll(sortBigDecimalsArray(array, sortKey));
            } else {
                // TODO :: throw error instead of generating message
                System.err.println("JSONUTIL : UNHANDLED KEY TYPE");
            }
        }
        return sorted;
    }

    public static JsonUtil getInstance() {
        if (instance == null) {
            instance = new JsonUtil();
        }
        return instance;
    }
}
