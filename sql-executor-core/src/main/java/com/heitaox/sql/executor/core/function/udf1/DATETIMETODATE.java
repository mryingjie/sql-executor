package com.heitaox.sql.executor.core.function.udf1;

import com.heitaox.sql.executor.core.util.DateUtils;

import java.time.LocalDateTime;

/**
 * created by Yingjie Zheng at 2019-08-30 10:13
 */
public class DATETIMETODATE extends UDF1<String,String>{
    @Override
    public String trans(String in) {
        return LocalDateTime.parse(in, DateUtils.dateTimeFormatter).toLocalDate().format(DateUtils.yyyyMMdd);
    }
}
