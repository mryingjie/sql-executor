package com.heitaox.sql.executor.core.function.udf1;


public class TRIM extends UDF1<String,String> {
    @Override
    public String trans(String in) {
        if(in == null){
            return null;
        }
        return in.trim();
    }
}
