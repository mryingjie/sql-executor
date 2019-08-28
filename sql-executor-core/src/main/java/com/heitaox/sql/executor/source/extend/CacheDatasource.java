package com.heitaox.sql.executor.source.extend;

import com.heitaox.sql.executor.core.entity.PredicateEntity;
import com.heitaox.sql.executor.core.util.DataFrameUntil;
import com.heitaox.sql.executor.source.DataSource;
import joinery.DataFrame;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("unchecked")
@Slf4j
public class CacheDatasource implements DataSource {

    private volatile Map<String, DataFrame<Object>> cacheDataSource = new ConcurrentHashMap<>();

    @Override
    public DataFrame queryAll(String tableNme, String tableAlias) {
        DataFrame dataFrame = cacheDataSource.get(tableNme);
        DataFrameUntil.setColumnTableAlias(dataFrame, tableAlias);
        return dataFrame;
    }

    @Override
    public int insert(List<Map<String, Object>> valueList, String tableName) {
        DataFrame dataFrame = cacheDataSource.get(tableName);
        if (dataFrame == null) {
            log.error("can not find dataSource[{}] from CacheDatasource", tableName);
            return 0;
        }
        Set columns = dataFrame.columns();
        List<Object> row = new ArrayList<>(columns.size());
        for (Map<String, Object> map : valueList) {
            for (Object column : columns) {
                row.add(map.get(column));
            }
            dataFrame.append(row);
            row.clear();
        }
        return valueList.size();
    }

    @Override
    public int update(Map<String, Object> updateItems, List<PredicateEntity<Object>> predicateEntities, String tableName) throws IOException {
        DataFrame<Object> df = cacheDataSource.get(tableName);
        if (df == null) {
            log.error("can not find dataSource[{}] from CacheDatasource", tableName);
            return 0;
        }

        Map<String, Integer> columnToIndex = DataFrameUntil.computeFiledToIndex(df);
        int update = 0;
        DataFrame<Object> dataFrame = new DataFrame<>(Arrays.asList(df.columns().toArray()));
        for (List<Object> values : df) {
            if (DataFrameUntil.exectuePredicaeEntity(predicateEntities, columnToIndex, values, tableName)) {
                List<Object> newRow = new ArrayList<>(values);
                for (Map.Entry<String, Object> entry : updateItems.entrySet()) {
                    Object value = entry.getValue();
                    newRow.set(columnToIndex.get(entry.getKey()), value);
                }
                dataFrame.append(newRow);
                update ++;
            } else {
                dataFrame.append(values);

            }
        }
        cacheDataSource.put(tableName, dataFrame);

        return update;
    }

    @Override
    public int delete(List<PredicateEntity<Object>> predicateEntities, String tableName) {
        DataFrame<Object> df = cacheDataSource.get(tableName);
        if (df == null) {
            log.warn("can not find dataSource[{}] from CacheDatasource", tableName);
            return 0;
        }

        Map<String, Integer> columnToIndex = DataFrameUntil.computeFiledToIndex(df);
        int delete = 0;
        DataFrame<Object> dataFrame = new DataFrame<>(Arrays.asList(df.columns().toArray()));
        for (List<Object> values : df) {
            if (DataFrameUntil.exectuePredicaeEntity(predicateEntities, columnToIndex, values, tableName)) {
                delete ++;
            } else {
                dataFrame.append(values);
            }
        }
        cacheDataSource.put(tableName, dataFrame);

        return delete;
    }

    public boolean cache(String table, DataFrame df) {
        if (cacheDataSource.containsKey(table)) {
            log.warn("cache [{}] failed , this table already existed", table);
            return false;
        }
        cacheDataSource.put(table, df);
        return true;
    }

    public void removeCache(String table) {
        if (!cacheDataSource.containsKey(table)) {
            log.warn("the cache[{}] to delete does not exist!", table);
        } else {
            cacheDataSource.remove(table);
        }
    }
}
