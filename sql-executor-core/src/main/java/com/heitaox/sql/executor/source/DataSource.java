package com.heitaox.sql.executor.source;


import com.heitaox.sql.executor.core.entity.PredicateEntity;
import com.heitaox.sql.executor.core.executor.PredicateFilter;
import com.heitaox.sql.executor.core.util.DataFrameUntil;
import joinery.DataFrame;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @Author ZhengYingjie
 * @Date 2019-08-06
 * @Description abstract interface for all data sources
 */
@SuppressWarnings("unchecked")
public interface DataSource {

    /**
     * get all the data in the table and converting it to DataFrame.
     * @param tableNme tableName
     * @param tableAlias tableAlias
     * @return DataFrame
     */
    DataFrame queryAll(String tableNme, String tableAlias);

    /**
     * users can rewrite this method by getting the data based on the filter criteria
     * and converting it to the default implementation of DataFrame.
     * @see PredicateFilter
     * @param table tableNme
     * @param tableAlias tableAlias
     * @param predicateEntities Ascertain condition
     * @return DataFrame
     */
    default DataFrame queryByPredicate(String table, String tableAlias, List<PredicateEntity<Object>> predicateEntities){
        DataFrame<Object> df = queryAll(table, tableAlias);
        DataFrameUntil.setColumnTableAlias(df,tableAlias);
        if(predicateEntities == null || predicateEntities.size()==0){
            return df;
        }
        Map<String, Integer> columnToIndex = DataFrameUntil.computeFiledToIndex(df);
        return DataFrameUntil.filter(predicateEntities, df, columnToIndex, tableAlias);
    }

    /**
     * insert data into the data source
     * @param valueList valueList
     * @param tableName tableName
     * @return success count
     * @throws Exception
     */
    int insert(List<Map<String, Object>> valueList, String tableName) throws IOException;


    /**
     *
     * Update data to data source
     * @param updateItems updateItems
     * @param tableName tableName
     * @param predicateEntities predicateEntities
     * @return success count
     * @throws Exception
     */
    int update(Map<String, Object> updateItems, List<PredicateEntity<Object>> predicateEntities, String tableName) throws IOException;

    /**
     *
     * delete data from data source
     * @param tableName tableName
     * @param predicateEntities predicateEntities
     * @return success count
     * @throws Exception
     */
    int delete(List<PredicateEntity<Object>> predicateEntities, String tableName) throws IOException;
}
