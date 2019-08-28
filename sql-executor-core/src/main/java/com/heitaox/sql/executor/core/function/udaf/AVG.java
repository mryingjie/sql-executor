package com.heitaox.sql.executor.core.function.udaf;


import java.math.BigDecimal;

/**
 * @Author ZhengYingjie
 * @Date 2019-08-12
 * @Description
 */
@SuppressWarnings("all")
public class AVG extends UDAF<Number,Number> {

    private Long counter = 0L;

    @Override
    public Number getInitData() {
        return 0;
    }

    @Override
    public Number aggregate(Number input, Number aggre) {
        if(input == null){
            return aggre;
        }
        counter ++;
        Number res;
        if (input instanceof Double) {
            res = input.doubleValue() + aggre.doubleValue();
        } else if (input instanceof Float) {
            res = input.doubleValue() + aggre.doubleValue();
        } else if (input instanceof Integer) {
            res = input.longValue() + aggre.longValue();
        } else if (input instanceof Short) {
            res = input.longValue() + aggre.longValue();
        } else if (input instanceof Long) {
            res = input.longValue() + aggre.longValue();
        } else {
            res = input.doubleValue() + aggre.doubleValue();
        }
        return res;
    }

    @Override
    public Number computeResult(Number aggre) {
        return new BigDecimal(aggre.doubleValue()).divide(new BigDecimal(counter),2,BigDecimal.ROUND_HALF_UP).doubleValue();
    }
}