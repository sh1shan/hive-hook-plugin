package org.data.meta.hive.exceptions;

import java.util.Collections;
import java.util.List;

/**
 * @author chenchaolin
 * @date 2023-07-02
 */
public class NotificationException extends RuntimeException {
    private static final long serialVersionUID = -3342248364615189630L;

    private final List<String> failedMessages;

    public NotificationException(Exception e) {
        super(e);
        failedMessages = Collections.emptyList();
    }

    public NotificationException(Exception e, List<String> failedMessages) {
        super(e);
        this.failedMessages = failedMessages;
    }

    public List<String> getFailedMessages() {
        return failedMessages;
    }
}
