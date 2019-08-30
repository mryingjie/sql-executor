package com.heitaox.sql.executor.core.function.udf;

/**
 * created by Yingjie Zheng at 2019-08-30
 */
public abstract class UDF<U> {

    public static final String TRANS_METHOD = "trans";

    /**
     * @return final result
     */
    public abstract U trans();
}
