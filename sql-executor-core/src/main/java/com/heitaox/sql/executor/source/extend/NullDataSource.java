package com.heitaox.sql.executor.source.extend;


import com.heitaox.sql.executor.source.DataSource;
import com.heitaox.sql.executor.core.entity.PredicateEntity;
import joinery.DataFrame;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * As the identification data source of the intermediate table when the join query
 */
public  class NullDataSource implements DataSource {

    @Override
    public DataFrame queryAll(String tableNme, String tableAlias) {
        return null;
    }

    @Override
    public int insert(List<Map<String, Object>> valueList, String tableName) {
        return 0;
    }

    @Override
    public int update(Map<String, Object> updateItems, List<PredicateEntity<Object>> predicateEntities, String tableName) throws IOException {
        return 0;
    }

    @Override
    public int delete(List<PredicateEntity<Object>> predicateEntities, String tableName) {
        return 0;
    }


}
