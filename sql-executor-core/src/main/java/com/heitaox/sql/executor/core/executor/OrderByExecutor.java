package com.heitaox.sql.executor.core.executor;

import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.heitaox.sql.executor.core.analysis.SQLExprAnalyzer;
import com.heitaox.sql.executor.core.util.DataFrameUntil;
import joinery.DataFrame;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Slf4j
public class OrderByExecutor extends BaseExecutor{

    private SQLOrderBy orderBy;

    private Map<String, Integer> columnToIndex;

    @Override
    public DataFrame execute(DataFrame df) {
        List<String> orderByItemStrList = SQLExprAnalyzer.analysisOrderBy(orderBy);
        return executeOrderBy(df, orderByItemStrList);
    }

    private DataFrame executeOrderBy(DataFrame df, List<String> orderByItemStrList) {
        return DataFrameUntil.sortBy(df,columnToIndex,orderByItemStrList.toArray());
    }
}
