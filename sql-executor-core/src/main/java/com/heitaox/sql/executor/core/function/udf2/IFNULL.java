package com.heitaox.sql.executor.core.function.udf2;



public class IFNULL extends UDF2<Object,Object,Object> {
    @Override
    public Object trans(Object in1, Object in2) {
        return in1 == null ? in2 : in1;
    }
}
