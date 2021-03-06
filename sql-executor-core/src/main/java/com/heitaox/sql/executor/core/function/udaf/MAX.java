package com.heitaox.sql.executor.core.function.udaf;


public class MAX extends UDAF<Number,Number> {

    @Override
    public Number getInitData() {
        return Double.MIN_VALUE;
    }

    @Override
    public Number aggregate(Number input, Number aggre) {
        if(aggre == null){
            return aggre;
        }
        if(input.doubleValue() > aggre.doubleValue()){
            return input;
        }
        return aggre;
    }

    @Override
    public Number computeResult(Number aggre) {
        return aggre;
    }
}
