package org.data.meta.hive.util;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * @author chenchaolin
 * @date 2023-07-02
 */
public class PathUtils {

    private PathUtils() {
    }

    public static String getProjectPath() {
        String filePath = null;
        try {
            URL url = PathUtils.class.getProtectionDomain().getCodeSource().getLocation();
            if (url != null) {
                filePath = URLDecoder.decode(url.getPath(), StandardCharsets.UTF_8.name());
                filePath = filePath.replace("\\", "/");
                if (filePath.endsWith(".jar")) {
                    filePath = filePath.substring(0, filePath.lastIndexOf("/") + 1);
                }
                File file = new File(filePath);
                filePath = file.getAbsolutePath();
            }
        } catch (Exception e) {
            // ignore
            filePath = null;
        }
        return filePath;
    }

    public static void main(String[] args) {
        String projectPath = getProjectPath();
        System.out.println(projectPath);
    }
}
