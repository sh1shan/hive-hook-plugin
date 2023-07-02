package org.data.meta.hive.exceptions;

/**
 * @author chenchaolin
 * @date 2023-07-02
 */
public class SqlParseException extends RuntimeException {

    private static final long serialVersionUID = 7580524884309975467L;

    public SqlParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlParseException(String message) {
        super(message);
    }

    public SqlParseException(Throwable cause) {
        super(cause);
    }
}
