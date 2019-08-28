package com.heitaox.sql.executor.core.function.udaf;


import java.math.BigDecimal;

@SuppressWarnings("all")
public class SUM extends UDAF<Number, Number> {


    @Override
    public Number getInitData() {
        return 0;
    }

    @Override
    public Number aggregate(Number input, Number aggre) {
        Number res;
        if(input == null){
            return aggre;
        }
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
        if (aggre instanceof Double) {
            return new BigDecimal(aggre.doubleValue()).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
        } else if (aggre instanceof Float) {
            return new BigDecimal(aggre.doubleValue()).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
        }

        return aggre;
    }


}
