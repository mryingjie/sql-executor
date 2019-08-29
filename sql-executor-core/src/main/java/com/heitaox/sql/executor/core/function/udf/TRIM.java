package com.heitaox.sql.executor.core.function.udf;


public class TRIM extends UDF<String,String> {
    @Override
    public String trans(String in) {
        if(in == null){
            return null;
        }
        return in.trim();
    }
}
