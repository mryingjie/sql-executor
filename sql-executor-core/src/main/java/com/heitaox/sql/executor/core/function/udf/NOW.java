package com.heitaox.sql.executor.core.function.udf;

import com.heitaox.sql.executor.core.util.DateUtils;

import java.time.LocalDateTime;

/**
 * created by Yingjie Zheng at 2019-08-30 09:25
 */
public class NOW extends UDF<String> {
    @Override
    public String trans() {
        return DateUtils.dateTimeFormatter.format(LocalDateTime.now());
    }
}
