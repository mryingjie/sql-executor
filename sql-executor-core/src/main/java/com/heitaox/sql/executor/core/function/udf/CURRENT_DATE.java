package com.heitaox.sql.executor.core.function.udf;

import java.time.LocalDate;
import com.heitaox.sql.executor.core.util.DateUtils;

/**
 * created by Yingjie Zheng at 2019-08-30 09:16
 */
public class CURRENT_DATE extends UDF<String>{

    @Override
    public String trans() {
        return DateUtils.yyyyMMdd.format(LocalDate.now());
    }
}
