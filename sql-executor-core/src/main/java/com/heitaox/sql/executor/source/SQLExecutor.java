package com.heitaox.sql.executor.source;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.heitaox.sql.executor.core.analysis.SQLExprAnalyzer;
import com.heitaox.sql.executor.core.entity.PredicateEntity;
import com.heitaox.sql.executor.core.executor.*;
import com.heitaox.sql.executor.core.function.udaf.AVG;
import com.heitaox.sql.executor.core.function.udaf.COUNT;
import com.heitaox.sql.executor.core.function.udaf.MAX;
import com.heitaox.sql.executor.core.function.udaf.SUM;
import com.heitaox.sql.executor.core.function.udf3.CONCAT;
import com.heitaox.sql.executor.core.function.udf3.IF;
import com.heitaox.sql.executor.core.util.DataFrameUntil;
import com.heitaox.sql.executor.source.extend.CacheDatasource;
import joinery.DataFrame;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * sql执行器
 */
@Slf4j
@SuppressWarnings("unchecked")
public class SQLExecutor {

    //数据源
    private Map<String, DataSource> dataSources = new ConcurrentHashMap<>();

    private CacheDatasource cacheDatasource;

    private static final Map<String, Class> funcMap = new HashMap<>();

    private boolean filterBeforeJoin = false;

    private boolean filterFirst = false;

    static {
        funcMap.put("sum", SUM.class);
        funcMap.put("max", MAX.class);
        funcMap.put("avg", AVG.class);
        funcMap.put("count", COUNT.class);
        funcMap.put("if", IF.class);
        funcMap.put("CONCAT", CONCAT.class);
        funcMap.put("concat", CONCAT.class);
    }

    private SQLExecutor() {
    }

    public SQLExecutor(SQLExecutorBuilder builder) {
        this.dataSources = builder.dataSources;
        this.filterFirst = builder.enableFilterFirst;
        this.filterBeforeJoin = builder.enableFilterBeforeJoin;
        if (builder.enableCache) {
            this.enableCache();
        }
    }

    public boolean cache(String tableName, DataFrame df) {
        requireNonNull(cacheDatasource, "Please start the cache mode by calling enableCache()");
        if (cacheDatasource.cache(tableName, df)) {
            if (dataSources.containsKey(tableName)) {
                log.warn("cache [{}] failed , this table already existed", tableName);
                return false;
            }
            dataSources.put(tableName, cacheDatasource);
        } else {
            return false;
        }
        return true;
    }

    public void enableCache() {
        if (cacheDatasource != null) {
            log.warn("Cache mode is on, no need to open it again ！");
            return;
        }
        cacheDatasource = new CacheDatasource();
    }

    public boolean removeCache(String tableName) {
        DataSource dataSource = dataSources.get(tableName);
        if (dataSource == null) {
            log.warn("the cache[{}] to delete does not exist!", tableName);
            return true;
        }
        if (dataSource instanceof CacheDatasource) {
            dataSources.remove(tableName);
            ((CacheDatasource) dataSource).removeCache(tableName);
        } else {
            log.warn("The table to be deleted is not a cache,delete cache failed !!");
            return false;
        }
        return true;
    }

    /**
     * 启用join之前过滤的模式可以大大提高join查询的性能  但是要求sql的where语句必须指明字段属于哪个表，
     * 尽量不要使用十分复杂的过滤条件例如带括号的，
     * 以及两个表字段互相之间的比较(a.column1 = b.column2)，以及 or 等，
     * 否则结果不能得到保证。
     */
    private void enableFilterBeforeJoin() {
        filterBeforeJoin = true;
    }

    public boolean filterBeforeJoin() {
        return filterBeforeJoin;
    }

    /**
     * 启用先执行where过滤的模式 可以减少nosql数据库单表查询时的网络io同时提高查询性能，
     * 开启这个功能会同时开启filterBeforeJoin，详见enableFilterBeforeJoin()
     */
    private void enableFilterFirst() {
        this.filterBeforeJoin = true;
        this.filterFirst = true;
    }

    private void closeFilterFirst() {
        this.filterFirst = false;
    }

    public boolean filterFirst() {
        return filterFirst;
    }


    private void closeFilterBeforeJoin() {
        filterBeforeJoin = false;
    }

    public static void registFunc(String funcName, Class funcClass) {
        funcMap.put(funcName, funcClass);
    }

    public final static SQLExecutorBuilder sqlExecutorBuilder = new SQLExecutorBuilder();


    private DataFrame executeQuery(SQLSelectQueryBlock sqlSelectQueryBlock) {
        Map<String, Integer> columnToIndex;
        DataFrame df = null;
        //一、 判断查询类型并加载数据
        SQLTableSource table = sqlSelectQueryBlock.getFrom();
        if (table instanceof SQLExprTableSource) {
            final String tableAlias = table.computeAlias(); //有别名就是别名 没有就是表名
            //普通单表查询
            String tableName = ((SQLExprTableSource) table).getName().getSimpleName();//表名
            DataSource dataSource = dataSources.get(tableName);
            if (dataSource instanceof FileDataSource) {
                //数据源是文件类型
                df = dataSource.queryAll(tableName, tableAlias);
                if (df == null || df.length() == 0) {//行数=0
                    return null;
                }
                columnToIndex = DataFrameUntil.computeFiledToIndex(df);
                df = analysisAndExecuteSimpleSQL(df, columnToIndex, sqlSelectQueryBlock, tableAlias, false);
            } else if (dataSource instanceof RDBMSDataSource) {
                df = ((RDBMSDataSource) dataSource).executeQuery(sqlSelectQueryBlock.toString());
            } else if (dataSource instanceof NoSQLDataSource) {
                if (filterFirst()) {
                    log.info("filter first");
                    SQLExpr where = sqlSelectQueryBlock.getWhere();
                    List<PredicateEntity<Object>> predicateEntities = SQLExprAnalyzer.analysisPredicate(where, this);
                    df = dataSource.queryByPredicate(tableName, tableAlias, predicateEntities);
                } else {
                    df = dataSource.queryAll(tableName, tableAlias);
                }
                columnToIndex = DataFrameUntil.computeFiledToIndex(df);
                df = analysisAndExecuteSimpleSQL(df, columnToIndex, sqlSelectQueryBlock, tableAlias, filterFirst());
            } else if (dataSource instanceof CacheDatasource) {
                df = dataSource.queryAll(tableName, tableAlias);
                if (df == null || df.length() == 0) {//行数=0
                    return null;
                }
                columnToIndex = DataFrameUntil.computeFiledToIndex(df);
                df = analysisAndExecuteSimpleSQL(df, columnToIndex, sqlSelectQueryBlock, tableAlias, false);
            } else {
                throw new RuntimeException("unknow dataSource Type of " + dataSource.getClass().getTypeName());
            }

        } else if (table instanceof SQLJoinTableSource) {
            //join 查询
            df = new JoinExecutor((SQLJoinTableSource) table, dataSources, this, sqlSelectQueryBlock).execute(df);
            if (df == null) {
                return new DataFrame();
            }
            columnToIndex = DataFrameUntil.computeFiledToIndex(df);
            df = analysisAndExecuteSimpleSQL(
                    df,
                    columnToIndex,
                    sqlSelectQueryBlock,
                    JoinExecutor.JOIN_TABLE_ALIAS,
                    this.filterBeforeJoin
            );
        } else if (table instanceof SQLSubqueryTableSource) {
            //子查询
            SQLSubqueryTableSource tableSource = (SQLSubqueryTableSource) table;
            String subSql = tableSource.getSelect().toString();
            df = executeQuery(subSql);
            DataFrameUntil.setColumnTableAlias(df, tableSource.getAlias());
            columnToIndex = DataFrameUntil.computeFiledToIndex(df);
            df = analysisAndExecuteSimpleSQL(df, columnToIndex, sqlSelectQueryBlock, tableSource.getAlias(), false);
        }
        return df == null ? new DataFrame() : df;
    }

    public DataFrame executeQuery(String sql) {
        requireNonNull(sql, "sql cannot be null");
        DataFrame<Object> df = null;
        //使用mysql的标准sql解析sql
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, "mysql");
        SQLStatement sqlStatement = sqlStatements.get(0);
        //查询语句
        if (sqlStatement instanceof SQLSelectStatement) {
            SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) sqlStatement;
            SQLSelectQuery query = sqlSelectStatement.getSelect().getQuery();
            if (query instanceof SQLSelectQueryBlock) {
                //非union类的查询
                df = executeQuery((SQLSelectQueryBlock) query);

            } else if (query instanceof SQLUnionQuery) {
                //union类型的查询
                SQLUnionQuery sqlUnionQuery = (SQLUnionQuery) query;
                DataFrame leftDf = executeQuery(sqlUnionQuery.getLeft().toString());//左边

                SQLSelectQueryBlock right = (SQLSelectQueryBlock) sqlUnionQuery.getRight();
                SQLOrderBy orderBy = right.getOrderBy();
                SQLLimit limit = right.getLimit();
                right.setOrderBy(null);
                right.setLimit(null);
                DataFrame rightDf = executeQuery(right);
                SQLUnionOperator operator = sqlUnionQuery.getOperator();
                Map<String, Integer> columnToIndex = null;
                assert rightDf != null;
                if (leftDf.length() != 0 && rightDf.length() != 0) {
                    columnToIndex = DataFrameUntil.computeFiledToIndex(rightDf);
                    df = DataFrameUntil.union(leftDf, rightDf, operator, columnToIndex);
                } else if (leftDf.length() != 0) {
                    columnToIndex = DataFrameUntil.computeFiledToIndex(rightDf);
                    df = leftDf;
                } else if (rightDf.length() != 0) {
                    columnToIndex = DataFrameUntil.computeFiledToIndex(rightDf);
                    df = rightDf;
                } else {
                    df = new DataFrame<>();
                }
                // 执行orderBy 对结果排序
                if (orderBy != null && columnToIndex != null) {
                    df = new OrderByExecutor(orderBy, columnToIndex).execute(df);
                }

                //执行 limit
                if (limit != null && columnToIndex != null) {
                    df = new LimitExecutor(limit).execute(df);
                }
            }

        } else {
            throw new RuntimeException("executeQuery only receive sql of query , please use other method");
        }

        // 重新命名表头 将xxx.  删除
        if (df != null) {
            DataFrameUntil.dropColumnTableAlias(df);
        }

        return df == null ? new DataFrame() : df;
    }


    private DataFrame<Object> analysisAndExecuteSimpleSQL(DataFrame<Object> df, Map<String, Integer> columnToIndex, SQLSelectQueryBlock sqlSelectQueryBlock, String tableAlias, boolean filtered) {
        // 一 执行where筛选
        if (!filtered) {
            SQLExpr where = sqlSelectQueryBlock.getWhere();
            if (where != null) {
                df = new PredicateFilter(where, columnToIndex, tableAlias, this).execute(df);
            }
        }

        // 二、执行group by
        SQLSelectGroupByClause groupBy = sqlSelectQueryBlock.getGroupBy();
        Map<List<Object>, List<Object>> keysToDataList;
        if (groupBy != null) {
            df = new GroupByExecutor(
                    groupBy,
                    sqlSelectQueryBlock.getSelectList(),
                    columnToIndex,
                    tableAlias,
                    funcMap
            ).execute(df);
        } else {
            df = new GroupByExecutor(
                    groupBy,
                    sqlSelectQueryBlock.getSelectList(),
                    columnToIndex,
                    tableAlias,
                    funcMap
            ).new FunctionExecutor()
                    .execute(DataFrameUntil.dfToMapList(df), false);
        }
        //更新字段名到数据的映射
        columnToIndex = DataFrameUntil.computeFiledToIndex(df);


        //五 执行distinct 暂不支持 使用groupBy 能实现相同的功能

        //六 执行orderBy 对结果排序
        SQLOrderBy orderBy = sqlSelectQueryBlock.getOrderBy();
        if (orderBy != null) {
            df = new OrderByExecutor(orderBy, columnToIndex).execute(df);
        }

        //执行 limit
        SQLLimit limit = sqlSelectQueryBlock.getLimit();
        if (limit != null) {
            df = new LimitExecutor(limit).execute(df);
        }


        return df;
    }

    public <T> List<T> executeQuery(String sql, Class<T> resultClass) throws Exception {
        if (resultClass == null) {
            throw new RuntimeException("resultClass can not be empty");
        }

        DataFrame df = executeQuery(sql);
        List<T> result = new ArrayList<>(df.length());
        for (int i = 0; i < df.length(); i++) {
            Constructor<T> constructor = resultClass.getConstructor();
            constructor.setAccessible(true);
            T t = constructor.newInstance();
            Field[] declaredFields = resultClass.getDeclaredFields();
            for (Field field : declaredFields) {
                field.setAccessible(true);
                field.set(t, df.get(i, field.getName()));
            }
            result.add(t);
        }
        return result;
    }

    /**
     * insert sql 如果不是关系型数据库必须包含列名称 insert into tableName (column1,column2) values(v1,v2);  不支持insert into tableName (v1,v2);的形式
     *
     * @param sql insert sql
     * @return success count
     * @throws IOException IOException
     */
    public int executeInsert(String sql) throws IOException {
        //使用mysql的标准sql解析sql
        int insert = 0;
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, "mysql");
        SQLStatement sqlStatement = sqlStatements.get(0);
        //查询语句
        if (sqlStatement instanceof SQLInsertStatement) {
            SQLInsertStatement insertStatement = (SQLInsertStatement) sqlStatement;
            String tableName = insertStatement.getTableName().getSimpleName();
            DataSource dataSource = dataSources.get(tableName);
            if (dataSource instanceof RDBMSDataSource) {
                //关系型数据库 直接执行sql
                insert = ((RDBMSDataSource) dataSource).executeUpdate(sql);
            } else {
                List<Map<String, Object>> valueList = SQLExprAnalyzer.analysisInsert(insertStatement);

                insert = dataSource.insert(valueList, tableName);
            }
        } else {
            throw new RuntimeException("executeInsert only receive sql of insert , please use other method");

        }

        return insert;
    }

    /**
     * 如果不是关系型数据库为数据源 将有以下限制：
     * 1、update sql中将仅支持set column = value格式，
     * 不支持value是个表达式的形式例如 column = value + 1
     * 2、update sql 中的where子句不支持or 括号 all any
     *
     * @param sql 执行的sql
     * @return success count
     * @throws Exception
     */
    public int executeUpdate(String sql) throws Exception {
        int update = 0;
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, "mysql");
        SQLStatement sqlStatement = sqlStatements.get(0);
        //更新语句
        if (sqlStatement instanceof SQLUpdateStatement) {
            SQLUpdateStatement updateStatement = (SQLUpdateStatement) sqlStatement;
            String tableName = updateStatement.getTableName().getSimpleName();
            DataSource dataSource = dataSources.get(tableName);
            if (dataSource instanceof RDBMSDataSource) {
                //关系型数据库 直接执行sql
                update = ((RDBMSDataSource) dataSource).executeUpdate(sql);
            } else {
                List<SQLUpdateSetItem> items = updateStatement.getItems();
                SQLExpr where = updateStatement.getWhere();
                Map<String, Object> updateItems = SQLExprAnalyzer.analysisUpdateItem(items);
                List<PredicateEntity<Object>> predicateEntities = SQLExprAnalyzer.analysisPredicate(where, this);
                update = dataSource.update(updateItems, predicateEntities, tableName);
            }
        } else {
            throw new RuntimeException("executeInsert only receive sql of update , please use other method");

        }

        return update;
    }

    public int executeDelete(String sql) throws IOException {
        int delete = 0;
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, "mysql");
        SQLStatement sqlStatement = sqlStatements.get(0);
        //删除语句
        if (sqlStatement instanceof SQLDeleteStatement) {
            SQLDeleteStatement deleteStatement = (SQLDeleteStatement) sqlStatement;
            String tableName = deleteStatement.getTableName().getSimpleName();
            DataSource dataSource = dataSources.get(tableName);
            if (dataSource instanceof RDBMSDataSource) {
                //关系型数据库 直接执行sql
                delete = ((RDBMSDataSource) dataSource).executeUpdate(sql);
            } else {
                SQLExpr where = deleteStatement.getWhere();
                List<PredicateEntity<Object>> predicateEntities = null;
                if (where != null) {
                    predicateEntities = SQLExprAnalyzer.analysisPredicate(where, this);

                }
                delete = dataSource.delete(predicateEntities, tableName);
            }
        } else {
            throw new RuntimeException("executeInsert only receive sql of delete , please use other method");

        }
        return delete;
    }


    public static class SQLExecutorBuilder {

        private Map<String, DataSource> dataSources = new ConcurrentHashMap<>();

        private boolean enableCache;

        private boolean enableFilterBeforeJoin;

        private boolean enableFilterFirst;

        public SQLExecutor build() {
            SQLExecutor sqlExecutor = new SQLExecutor();
            sqlExecutor.dataSources = this.dataSources;
            if(enableFilterFirst){
                sqlExecutor.enableFilterFirst();
            }
            if(enableFilterBeforeJoin){
                sqlExecutor.enableFilterBeforeJoin();
            }
            if (enableCache) {
                sqlExecutor.enableCache();
            }

            return sqlExecutor;
        }

        public SQLExecutorBuilder dataSources(Map<String, DataSource> dataSources) {
            this.dataSources = dataSources;
            return this;
        }

        public SQLExecutorBuilder putDataSource(String tableName, DataSource dataSource) {
            this.dataSources.put(tableName, dataSource);
            return this;
        }

        public SQLExecutorBuilder enableCache() {
            this.enableCache = true;
            return this;
        }

        public SQLExecutorBuilder enableFilterBeforeJoin() {
            this.enableFilterBeforeJoin = true;
            return this;
        }

        public SQLExecutorBuilder enableFilterFirst() {
            this.enableFilterFirst = true;
            return this;
        }


    }


}
