package com.heitaox.sql.executor.core.function.udf;


public class REVERSE extends UDF<String,String> {


    @Override
    public String trans(String in) {
        if(in == null){
            return null;
        }
        return new StringBuilder(in).reverse().toString();
    }
}
