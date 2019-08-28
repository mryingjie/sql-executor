package com.heitaox.sql.executor.source;

import com.heitaox.sql.executor.core.entity.PredicateEntity;
import com.heitaox.sql.executor.core.util.DataFrameUntil;
import com.heitaox.sql.executor.source.rdbms.StandardSqlDataSource;
import joinery.DataFrame;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RDBMSDataSource extends DataSource {

    /**
     * the sql execution of the general relational database
     * can directly use the execution engine of the database itself.
     * This method needs to implement the execution logic of the calling
     * database and convert it into a DataFrame.
     * example:
     *
     * @param sql Sql to execute query
     * @return DataFrame<Object>
     * @see StandardSqlDataSource#executeQuery(String)
     */
    DataFrame<Object> executeQuery(String sql);


    /**
     * call the relational database api to execute the update statement
     *
     * @param sql Sql to execute update
     * @return number of successful lines
     * @see StandardSqlDataSource#executeUpdate(String)
     */
    int executeUpdate(String sql);


    // /**
    //  *
    //  * example:
    //  * @see StandardSqlDataSource#executeQuery(String, Class)
    //  * @param sql Sql to execute
    //  * @param resClass type of data to be encapsulated per line of data
    //  * @param <T> Generic
    //  * @return List<T>
    //  */
    // <T> List<T> executeQuery(String sql,Class<T> resClass);


    /**
     * The default call to StandardSqlDataSource#executeQuery(String)
     * Perform a full table query.
     *
     * @param tableNme   tableName
     * @param tableAlias tableAlias
     * @return DataFrame
     * @see #executeQuery(String)
     * @see DataSource#queryAll(String, String)
     */
    @Override
    default DataFrame queryAll(String tableNme, String tableAlias) {
        String scanSql = "select * from " + tableNme;
        DataFrame<Object> df = executeQuery(scanSql);
        DataFrameUntil.setColumnTableAlias(df, tableAlias);
        return df;
    }

    /**
     * Convert to sql with a where clause and execute according to the incoming assertion condition
     * {@link PredicateEntity} Packaged assertion condition
     *
     * @param tableName         tableName
     * @param tableAlias        tableAlias
     * @param predicateEntities predicateEntities： Set of conditions
     * @return DataFrame
     * @see #executeQuery(String)
     * @see DataSource#queryByPredicate(String, String, List)
     */
    @Override
    default DataFrame queryByPredicate(String tableName, String tableAlias, List<PredicateEntity<Object>> predicateEntities) {
        if (predicateEntities == null || predicateEntities.size() == 0) {
            return queryAll(tableName, tableAlias);
        }
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(tableName).append(" ").append("where ");
        for (int i = predicateEntities.size() - 1; i >= 0; i--) {
            sb.append(predicateEntities.get(i).transToString());
        }
        DataFrame<Object> df = executeQuery(sb.toString());
        DataFrameUntil.setColumnTableAlias(df, tableAlias);
        return df;
    }


    /**
     * convert the wrapped valueList to sql and execute {@link #executeQuery(String)}
     *
     * @param valueList valueList of map
     * @param tableName tableName
     * @return number of successful lines
     */
    @Override
    default int insert(List<Map<String, Object>> valueList, String tableName) {
        if (valueList != null && valueList.size() > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append("insert into ").append(tableName).append("(");
            Map<String, Object> map = valueList.get(0);
            Set<String> columns = map.keySet();
            for (String column : columns) {
                sb.append(column).append(",");
            }
            sb.deleteCharAt(sb.lastIndexOf(",")).append(")").append(" values(");
            for (Map<String, Object> valueMap : valueList) {
                Collection<Object> values = valueMap.values();
                for (Object value : values) {
                    sb.append(value).append(",");
                }
            }
            sb.deleteCharAt(sb.lastIndexOf(",")).append(")");
            return executeUpdate(sb.toString());
        } else {
            return 0;

        }
    }

    @Override
    default int update(Map<String, Object> updateItems, List<PredicateEntity<Object>> predicateEntities, String tableName) throws IOException {
        if (updateItems != null && updateItems.size() > 0) {
            final StringBuilder sb = new StringBuilder("update ");
            sb.append(tableName).append(" ").append("set ");
            updateItems.forEach((s, o) -> {
                sb.append(s).append("=");
                if (o instanceof String) {
                    sb.append("'").append(o).append("'");
                } else {
                    sb.append(o).append(",");
                }
            });
            sb.deleteCharAt(sb.lastIndexOf(","));
            if (predicateEntities != null && predicateEntities.size() > 0) {
                sb.append(" where ");
                predicateEntities.forEach((entity -> sb.append(entity.transToString())));
            }
            return executeUpdate(sb.toString());
        } else {
            return 0;
        }
    }

    @Override
    default int delete(List<PredicateEntity<Object>> predicateEntities, String tableName) {
        final StringBuilder sb = new StringBuilder("delete from ");
        sb.append(tableName);
        if (predicateEntities != null && predicateEntities.size() > 0) {
            sb.append(" where ");
            predicateEntities.forEach((entity -> sb.append(entity.transToString())));
        }
        return executeUpdate(sb.toString());
    }
}
