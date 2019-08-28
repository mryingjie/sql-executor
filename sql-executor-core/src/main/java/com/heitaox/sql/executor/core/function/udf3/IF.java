package com.heitaox.sql.executor.core.function.udf3;


/**
 * @Author ZhengYingjie
 * @Date 2019-08-19
 * @Description
 */
public class IF extends UDF3<Boolean, Object,Object,Object>{
    @Override
    public Object trans(Boolean condition, Object in1, Object in2) {
        if(condition){
            return in1;
        }else {
            return in2;
        }
    }
}
