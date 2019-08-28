package com.heitaox.sql.executor.core.function.udaf;

/**
 * @Author ZhengYingjie
 * @Date 2019-08-13
 * @Description
 */
public class COUNT extends UDAF<Object,Number>{
    @Override
    public Number getInitData() {
        return 0;
    }

    @Override
    public Number aggregate(Object input, Number aggre) {
        if(input != null){
            aggre = aggre.intValue() + 1;
        }
        return aggre;
    }

    @Override
    public Number computeResult(Number aggre) {
        return aggre;
    }
}
