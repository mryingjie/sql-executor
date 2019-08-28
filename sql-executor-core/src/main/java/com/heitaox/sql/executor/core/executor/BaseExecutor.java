package com.heitaox.sql.executor.core.executor;

import joinery.DataFrame;

public abstract class BaseExecutor {

    public abstract DataFrame execute(DataFrame df);
}
