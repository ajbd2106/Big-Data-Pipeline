package mjaksic.Kafka_To_Hive.json;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.*;

/**
 * Transforms JSON constructs into Java data structures (collections).
 */
public class Parser {

    /**
     * A convenience method. See the link below.
     * @param JSON_strings JSON strings.
     * @return Maps.
     * @see bigdata.json.Parser#FlatJSONToMap(String)
     */
    public static List<Map<String, String>> FlatJSONToMap (List<String> JSON_strings) {
        ArrayList<Map<String, String>> parsed_strings = new ArrayList<>();
        for (String string:JSON_strings) {
            Map<String, String> data = Parser.FlatJSONToMap(string);
            parsed_strings.add(data);
        }
        return parsed_strings;
    }

    /**
     * Transforms a "flat" JSON string into a Java data structure (collection).
     * @param JSON_string A "flat" JSON string: "{"key_one": "value_one", "key_two": "value_two", ...}"
     * @return A Map: {key_one=value_one, key_two=value_two, ...}
     */
    public static Map<String, String> FlatJSONToMap(String JSON_string) {
        Gson json_parser = new Gson();
        Type map_template = Parser.GetMapTemplate();

        HashMap<String, String> JSON_map = json_parser.fromJson(JSON_string, map_template);
        return JSON_map;
    }

    /**
     *
     * @return A template used by Gson.
     * @see <a href="https://github.com/google/gson/blob/master/UserGuide.md">Gson User Guide</a>
     */
    private static Type GetMapTemplate() {
        return new TypeToken<HashMap<String, String>>(){}.getType();
    }
}
