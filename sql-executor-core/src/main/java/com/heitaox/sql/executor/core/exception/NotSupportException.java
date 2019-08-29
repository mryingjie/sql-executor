package com.heitaox.sql.executor.core.exception;

/**
 * @Author ZhengYingjie
 * @Date 2019-08-29
 * @Description
 */
public class NotSupportException extends RuntimeException{

    public NotSupportException() {
        super();
    }

    public NotSupportException(String message) {
        super(message);
    }

    public NotSupportException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotSupportException(Throwable cause) {
        super(cause);
    }

    protected NotSupportException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
