package org.data.meta.hive.exceptions;


public class HiveHookException extends RuntimeException {

    private static final long serialVersionUID = 4209504057102032847L;

    public HiveHookException() {
    }

    public HiveHookException(String message) {
        super(message);
    }

    public HiveHookException(String message, Throwable cause) {
        super(message, cause);
    }

    public HiveHookException(Throwable cause) {
        super(cause);
    }

    public HiveHookException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
