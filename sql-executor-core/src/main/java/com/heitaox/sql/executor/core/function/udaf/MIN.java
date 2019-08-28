package com.heitaox.sql.executor.core.function.udaf;

/**
 * @Author ZhengYingjie
 * @Date 2019-08-12
 * @Description
 */
public class MIN extends UDAF<Number,Number>{

    @Override
    public Number getInitData() {
        return Double.MAX_VALUE;
    }

    @Override
    public Number aggregate(Number input, Number aggre) {
        if(input == null){
            return aggre;
        }
        if(input.doubleValue() < aggre.doubleValue()){
            return input;
        }
        return aggre;
    }

    @Override
    public Number computeResult(Number aggre) {
        return aggre;
    }
}
