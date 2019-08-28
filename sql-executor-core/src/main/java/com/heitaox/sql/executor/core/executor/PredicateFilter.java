package com.heitaox.sql.executor.core.executor;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.heitaox.sql.executor.core.analysis.SQLExprAnalyzer;
import com.heitaox.sql.executor.core.entity.PredicateEntity;
import com.heitaox.sql.executor.core.util.DataFrameUntil;
import com.heitaox.sql.executor.source.SQLExecutor;
import joinery.DataFrame;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * @Author ZhengYingjie
 * @Date 2019-08-14
 * @Description
 */
@AllArgsConstructor
@Slf4j
public class PredicateFilter extends BaseExecutor {

    private SQLExpr where;

    private Map<String, Integer> columnToIndex;

    private String tableAlias;

    private SQLExecutor sqlExecutor;

    @Override
    public DataFrame execute(DataFrame df) {
        List<PredicateEntity<Object>> predicateWhereEntities = SQLExprAnalyzer.analysisPredicate(where, sqlExecutor);
        return executePredicate(predicateWhereEntities, df, columnToIndex);
    }


    private DataFrame executePredicate(List<PredicateEntity<Object>> predicateEntities, DataFrame<Object> df, Map<String, Integer> columnToIndex) {
        return DataFrameUntil.filter(predicateEntities, df, columnToIndex, tableAlias);
    }




}
