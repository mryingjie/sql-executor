package com.heitaox.sql.executor.core.executor;

import joinery.DataFrame;

/**
 * @Author ZhengYingjie
 * @Date 2019-08-14
 * @Description
 */
public abstract class BaseExecutor {

    public abstract DataFrame execute(DataFrame df);
}
