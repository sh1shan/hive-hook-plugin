package org.data.meta.hive.service.notification;

import org.apache.commons.configuration2.Configuration;
import org.data.meta.hive.ApplicationProperties;

/**
 * @author chenchaolin
 * @date 2023-07-02
 */
public class NotificationProvider {
    private static KafkaNotification kafkaNotification;

    public static KafkaNotification get() {
        if (kafkaNotification == null) {
            Configuration applicationProperties = ApplicationProperties.get();
            kafkaNotification = new KafkaNotification(applicationProperties);
        }
        return kafkaNotification;
    }
}
