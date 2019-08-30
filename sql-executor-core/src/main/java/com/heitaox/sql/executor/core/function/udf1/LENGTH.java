package com.heitaox.sql.executor.core.function.udf1;

public class LENGTH extends UDF1<String,Integer> {
    @Override
    public Integer trans(String in) {
        if(in == null){
            return 0;
        }
        return in.length();
    }
}
