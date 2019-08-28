package com.heitaox.sql.executor.core.function.udf;

/**
 * @Author ZhengYingjie
 * @Date 2019-08-09
 * @Description
 * @param <I> input parameter type
 * @param <U> output parameter type
 * the abstract class of the UDF function,
 * all udf functions must inherit this class and
 * implement all the abstract methods in the class.
 */
public abstract class UDF<I,U> {

    public static final String TRANS_METHOD = "trans";

    /**
     * Convert the input parameters to the final result
     * @param in input parameter
     * @return final result
     */
    public abstract U trans(I in);
}
