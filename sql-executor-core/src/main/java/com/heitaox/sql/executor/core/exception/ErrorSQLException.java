package com.heitaox.sql.executor.core.exception;

/**
 * @Author ZhengYingjie
 * @Date 2019-08-29
 * @Description
 */
public class ErrorSQLException extends RuntimeException {
    public ErrorSQLException() {
        super();
    }

    public ErrorSQLException(String message) {
        super(message);
    }

    public ErrorSQLException(String message, Throwable cause) {
        super(message, cause);
    }

    public ErrorSQLException(Throwable cause) {
        super(cause);
    }

    protected ErrorSQLException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
