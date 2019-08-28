package com.heitaox.sql.executor.core.function.udaf;

/**
 * @param <I> input parameter type
 * @param <U> output parameter type
 * the abstract class of the UDAF function,
 * all aggregate functions must inherit this class and
 * implement all the abstract methods in the class.
 *
 */
public abstract class UDAF<I, U> {

    public static final String INIT_MATHOD_NAME = "getInitData";

    public static final String AGGREGATE_METHOD_NAME = "aggregate";

    public static final String COMPUTE_RESULT_METHOD_NAME = "computeResult";

    /**
     * Initial value of the aggregate function
     * @return Initial value
     */
    public abstract U getInitData();

    /**
     * Aggregate the input parameters according to a certain logic and
     * the results after the last aggregation
     * @param input input parameter
     * @param aggre Results after the last aggregation
     * @return Results after this aggregation
     */
    public abstract U aggregate(I input, U aggre);

    /**
     * Calculate the final result using the results obtained after the final polymerization
     * @param aggre Result after final polymerization
     * @return final result
     */
    public abstract U computeResult(U aggre);
}
