package com.heitaox.sql.executor.core.function.udf3;


public class SUBSTRING extends UDF3<String,Integer,Integer,String> {
    @Override
    public String trans(String in1, Integer in2, Integer in3) {
        if(in1==null){
            return null;
        }

        return in1.substring(in2, in3 + in2);
    }

}
