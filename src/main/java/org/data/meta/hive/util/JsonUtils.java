package org.data.meta.hive.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonUtils {
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

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
}
