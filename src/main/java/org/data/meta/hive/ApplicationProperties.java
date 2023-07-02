package org.data.meta.hive;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.data.meta.hive.exceptions.HiveHookException;
import org.data.meta.hive.util.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.Iterator;

/**
 * Application properties
 *
 * @author chenchaolin
 * @date 2023-07-02
 */
public final class ApplicationProperties {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationProperties.class);

    public static final String APPLICATION_PROPERTIES = "kafka.properties";

    private static volatile Configuration instance = null;

    private ApplicationProperties() {

    }

    public static void forceReload() {
        if (instance != null) {
            synchronized (ApplicationProperties.class) {
                if (instance != null) {
                    instance = null;
                }
            }
        }
    }

    public static Configuration get() {
        if (instance == null) {
            synchronized (ApplicationProperties.class) {
                if (instance == null) {
                    instance = get(APPLICATION_PROPERTIES);
                }
            }
        }
        return instance;
    }

    public static Configuration get(String fileName) {
        // get from jar path
        String confLocation = PathUtils.getProjectPath();
        try {
            URL url;
            File file = new File(confLocation, fileName);
            if (confLocation == null || !file.exists()) {
                LOG.info("Looking for {} in classpath", fileName);
                url = ApplicationProperties.class.getClassLoader().getResource(fileName);
                if (url == null) {
                    LOG.info("Looking for /{} in classpath", fileName);
                    url = ApplicationProperties.class.getClassLoader().getResource("/" + fileName);
                }
            } else {
                url = file.toURI().toURL();
            }
            LOG.info("Loading {} from {}", fileName, url);
            Parameters params = new Parameters();
            FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                    new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                            .configure(params.properties()
                                    .setURL(url));
            FileBasedConfiguration configuration = builder.getConfiguration();
            logConfiguration(configuration);
            return configuration;
        } catch (Exception e) {
            throw new HiveHookException("Failed to load application properties", e);
        }
    }

    private static void logConfiguration(Configuration configuration) {
        if (LOG.isDebugEnabled()) {
            Iterator<String> keys = configuration.getKeys();
            LOG.debug("Configuration loaded:");
            while (keys.hasNext()) {
                String key = keys.next();
                LOG.debug("{} = {}", key, configuration.getProperty(key));
            }
        }
    }
}
