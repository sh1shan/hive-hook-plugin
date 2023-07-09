package org.data.meta.hive.service.notification;


import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.configuration2.Configuration;
import org.data.meta.hive.exceptions.NotificationException;

import java.util.Arrays;
import java.util.List;

/**
 * @author chenchaolin
 * @date 2023-07-02
 */
public abstract class AbstractNotification implements NotificationInterface {

    /**
     * each char can encode upto 4 bytes in UTF-8
     */
    public static final int MAX_BYTES_PER_CHAR = 4;

    public AbstractNotification() {
    }

    @Override
    public void send(List<String> messages) {
        sendInternal(messages);
    }

    @Override
    public void send(String... messages) {
        send(Arrays.asList(messages));
    }

    /**
     * Send the given messages.
     *
     * @param messages the array of messages to send
     * @throws NotificationException if an error occurs while sending
     */
    protected abstract void sendInternal(List<String> messages);
}
