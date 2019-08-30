package com.heitaox.sql.executor.core.function.udf;

/**
 * created by Yingjie Zheng at 2019-08-30 09:26
 */
public class UNIX_TIMESTAMP extends UDF<Long> {
    @Override
    public Long trans() {
        return System.currentTimeMillis();
    }
}
