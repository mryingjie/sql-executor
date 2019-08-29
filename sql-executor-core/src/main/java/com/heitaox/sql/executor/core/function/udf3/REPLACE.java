package com.heitaox.sql.executor.core.function.udf3;



public class REPLACE extends UDF3<String,String,String,String> {

    @Override
    public String trans(String in1, String in2, String in3) {
        if(in1 == null){
            return  null;
        }
        return in1.replace(in2, in3);
    }

}
