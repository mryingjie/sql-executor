package com.heitaox.sql.executor.core.function.udf;

public class LENGTH extends UDF<String,Integer> {
    @Override
    public Integer trans(String in) {
        if(in == null){
            return 0;
        }
        return in.length();
    }
}
