package com.heitaox.sql.executor.core.function.udf1;


import com.heitaox.sql.executor.core.util.DateUtils;

import java.time.LocalDate;

/**
 * created by Yingjie Zheng at 2019-08-30 09:38
 */
public class MONTH extends UDF1<String,Integer> {

    @Override
    public Integer trans(String in) {

        return LocalDate.parse(in,DateUtils.yyyyMMdd).getMonthValue();
    }


    public static void main(String[] args) {
        System.out.println(LocalDate.parse("2019-08-3 09:38:21",DateUtils.yyyyMMdd));
    }


}
