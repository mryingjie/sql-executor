package com.heitaox.sql.executor.core.function.udf1;

import com.heitaox.sql.executor.core.util.DateUtils;

import java.time.LocalDate;

/**
 * created by Yingjie Zheng at 2019-08-30 10:04
 */
public class YEAR extends UDF1<String,Integer> {
    @Override
    public Integer trans(String in) {
        return LocalDate.parse(in, DateUtils.yyyyMMdd).getYear();
    }
}
