package com.heitaox.sql.executor.core.function.udf1;


import com.heitaox.sql.executor.core.util.DateUtils;

import java.util.Calendar;
import java.util.Date;

/**
 * created by Yingjie Zheng at 2019-08-30 09:51
 */
public class WEEK extends UDF1<String, Integer> {
    @Override
    public Integer trans(String in) {
        Date date = DateUtils.formateDateStr(in);

        Calendar calendar = Calendar.getInstance();
        calendar.setFirstDayOfWeek(Calendar.MONDAY);
        calendar.setTime(date);

        return calendar.get(Calendar.WEEK_OF_YEAR);
    }
}
