package com.heitaox.sql.executor.core.function.udf2;

/**
 * @Author ZhengYingjie
 * @Date 2019/8/9
 * @Description
 * @param <I> input parameter type
 * @param <R> input parameter type
 * @param <U> output parameter type
 * the abstract class of the UDF2 function,
 * all udf2 functions must inherit this class and
 * implement all the abstract methods in the class.
 */
public abstract class UDF2<I,R,U> {

    public static final String TRANS_METHOD = "trans";

    /**
     * Convert the input parameters to the final result
     * @param in1 input parameter
     * @param in2 input parameter
     * @return final result
     */
    public abstract U trans(I in1,R in2);
}
