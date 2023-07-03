package org.data.meta.hive.service.notification;


/**
 * @author chenchaolin
 * @date 2023-07-02
 */
public class NotificationProvider {
    private static KafkaNotification kafkaNotification;

    public static KafkaNotification get() {
        if (kafkaNotification == null) {
            kafkaNotification = new KafkaNotification();
        }
        return kafkaNotification;
    }
}
