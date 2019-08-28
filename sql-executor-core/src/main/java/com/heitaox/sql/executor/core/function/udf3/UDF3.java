package com.heitaox.sql.executor.core.function.udf3;


/**
 * @Author ZhengYingjie
 * @Date 2019-08-19
 * @Description
 * @param <I> input parameter type
 * @param <R> input parameter type
 * @param <T> input parameter type
 * @param <U> output parameter type
 * the abstract class of the UDF2 function,
 * all udf2 functions must inherit this class and
 * implement all the abstract methods in the class.
 */
public abstract class UDF3<I,R,T,U> {

    public static final String TRANS_METHOD = "trans";

    /**
     * Convert the input parameters to the final result
     * @param in1 input parameter
     * @param in2 input parameter
     * @param in3 input parameter
     * @return final result
     */
    public abstract U trans(I in1, R in2, T in3);
}
