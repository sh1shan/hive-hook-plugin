package org.data.meta.hive.util;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

import java.util.*;

public class JsonUtils {
    //使用这种方式避免json转换过程中导致 等号、双引号、单引号被错误翻译成其他乱码
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private static final JsonParser parser = new JsonParser();

    public JsonUtils() {
    }

    /**
     * 对象转换成JSON字符串
     *
     * @param o Object
     * @return String
     */
    public static String toJsonString(Object o) {
        return GSON.toJson(o);
    }


    /**
     * JSON转换成Java对象
     *
     * @param json  String
     * @param clazz Class<T>
     * @param <T>   T
     * @return T
     */
    public <T> T fromJson(String json, Class<T> clazz) {
        if (json == null || json.trim().length() == 0) {
            return null;
        }
        return GSON.fromJson(json, clazz);
    }


    /**
     * JSON转换成Java对象
     *
     * @param json String
     * @return Map
     */
    public Map<String, Object> json2Map(String json) {
        java.lang.reflect.Type type =
                new TypeToken<HashMap<String, Object>>() {
                }.getType();
        return GSON.fromJson(json, type);
    }

    /**
     * 如果jsons 是数组格式，则挨个转换成clazz对象返回list，否则直接尝试转换成clazz对象返回list
     *
     * @param jsons jsons
     * @param clazz Class<T>
     * @param <T>   T
     * @return T
     */
    public <T> List<T> fromJsons(String jsons, Class<T> clazz) {
        if (jsons == null || jsons.trim().length() == 0) {
            return Collections.emptyList();
        }
        List<T> list = new ArrayList<>();
        JsonElement jsonNode = parser.parse(jsons);
        if (jsonNode.isJsonArray()) {
            JsonArray array = jsonNode.getAsJsonArray();
            for (int i = 0; i < array.size(); i++) {
                list.add(GSON.fromJson(array.get(i), clazz));
            }
        } else {
            list.add(fromJson(jsons, clazz));
        }
        return list;
    }
}
