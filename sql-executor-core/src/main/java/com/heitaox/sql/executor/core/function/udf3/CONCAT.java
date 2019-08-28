package com.heitaox.sql.executor.core.function.udf3;


/**
 * @Author ZhengYingjie
 * @Date 2019-08-26
 * @Description
 */
public class CONCAT extends UDF3<String,String,String,String>{
    @Override
    public String trans(String in1, String in2, String in3) {
        return in1 + in2 + in3;
    }
}
