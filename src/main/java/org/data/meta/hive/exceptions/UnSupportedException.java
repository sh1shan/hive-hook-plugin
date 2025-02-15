package org.data.meta.hive.exceptions;


public class UnSupportedException extends RuntimeException {
    private static final long serialVersionUID = -6917876792327031646L;

    public UnSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnSupportedException(String message) {
        super(message);
    }

    public UnSupportedException(Throwable cause) {
        super(cause);
    }
}
