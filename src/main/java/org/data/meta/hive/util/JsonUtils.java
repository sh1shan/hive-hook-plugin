package org.data.meta.hive.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonUtils {
    //使用这种方式避免json转换过程中导致 等号、双引号、单引号被错误翻译成其他乱码
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
