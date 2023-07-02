package org.data.meta.hive.service.notification;

import java.util.List;

import org.data.meta.hive.exceptions.NotificationException;


/**
 * @author chenchaolin
 * @date 2023-07-02
 */
public interface NotificationInterface {
    /**
     * Send the given messages.
     *
     * @param messages the messages to send
     * @throws NotificationException if an error occurs while sending
     */
    void send(String... messages);

    /**
     * Send the given messages.
     *
     * @param messages the list of messages to send
     * @throws NotificationException if an error occurs while sending
     */
    void send(List<String> messages);

    /**
     * Shutdown any notification producers and consumers associated with this interface instance.
     */
    void close();
}
