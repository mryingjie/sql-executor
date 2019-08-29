package com.heitaox.sql.executor.core.function.udf2;

/**
 *
 */
public class LEFT extends UDF2<String,Integer,String>{
    @Override
    public String trans(String in1, Integer in2) {
        if(in1 == null){
            return null;
        }
        if(in1.length()<=in2){
            return in1;
        }
        return in1.substring(0, in2);
    }

}
