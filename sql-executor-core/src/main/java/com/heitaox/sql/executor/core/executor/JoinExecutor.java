package com.heitaox.sql.executor.core.executor;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.heitaox.sql.executor.core.analysis.SQLExprAnalyzer;
import com.heitaox.sql.executor.core.entity.JoinTableEntity;
import com.heitaox.sql.executor.core.entity.PredicateEntity;
import com.heitaox.sql.executor.core.exception.NotSupportException;
import com.heitaox.sql.executor.core.util.DataFrameUntil;
import com.heitaox.sql.executor.source.DataSource;
import com.heitaox.sql.executor.SQLExecutor;
import com.heitaox.sql.executor.source.extend.NullDataSource;
import joinery.DataFrame;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Slf4j
@SuppressWarnings("all")
public class JoinExecutor extends BaseExecutor {

    public static final String JOIN_TABLE_ALIAS = "joinTable";

    private static ExecutorService executorService;


    private SQLJoinTableSource joinTables;

    private Map<String, DataSource> dataSources;


    private Map<String, DataFrame> cache = new HashMap<>();

    private final SQLExecutor sqlExecutor;

    private final SQLSelectQueryBlock sqlSelectQueryBlock;

    public JoinExecutor(SQLJoinTableSource joinTables, Map<String, DataSource> dataSources, SQLExecutor sqlExecutor, SQLSelectQueryBlock sqlSelectQueryBlock) {
        this.joinTables = joinTables;
        this.dataSources = dataSources;
        this.sqlExecutor = sqlExecutor;
        this.sqlSelectQueryBlock = sqlSelectQueryBlock;
    }


    @Override
    public DataFrame execute(DataFrame df) {
        List<JoinTableEntity> joinTableEntities = SQLExprAnalyzer.analysisJoinTable(joinTables);
        List<JoinTableEntity> joinGroup = new ArrayList<>();
        for (int i = joinTableEntities.size() - 1; i >= 0; i--) {
            JoinTableEntity joinTableEntity = joinTableEntities.get(i);
            if (joinGroup.size() != 3) {
                joinGroup.add(joinTableEntity);
            } else {
                df = baseExecute(df, joinGroup);
                joinGroup.clear();
                dataSources.put("tmp" + i, new NullDataSource());
                joinGroup.add(new JoinTableEntity("tmp" + i, "tmp" + i));
                joinGroup.add(joinTableEntity);
            }
        }

        return baseExecute(df, joinGroup);
    }


    private DataFrame baseExecute(DataFrame df, List<JoinTableEntity> joinTableEntities) {

        JoinTableEntity leftTable = joinTableEntities.get(0);
        JoinTableEntity joinTypeAndCondation = joinTableEntities.get(1);
        JoinTableEntity rightTable = joinTableEntities.get(2);

        //准备数据
        DataFrame leftDf = null;
        DataFrame rightDf = null;
        if (!(dataSources.get(leftTable.getTName()) instanceof NullDataSource)) {
            // 两个表都需要加载外部数据 使用两个线程同时加载
            if (executorService == null) {
                executorService = new ThreadPoolExecutor(
                        2, //核心线程数2
                        Runtime.getRuntime().availableProcessors() * 2, //最大线程=cpu核数*2
                        60, //非核心线程600秒内无任务被销毁
                        //任务缓存队列最多缓存cpu核数*2个任务
                        TimeUnit.SECONDS,new LinkedBlockingQueue(Runtime.getRuntime().availableProcessors() * 2),
                        // 当线程池的任务缓存队列已满并且线程池中的线程数目达到最大值时由调用线程处理该任务
                        new ThreadPoolExecutor.CallerRunsPolicy()
                );
            }
            //判断是否启用join前过滤
            Future<DataFrame> leftDfFuture;
            Future<DataFrame> rightDfFuture;
            if (sqlExecutor.filterBeforeJoin()) {
                log.info("enable filterBeforeJoin ");
                SQLExpr where = sqlSelectQueryBlock.getWhere();
                List<PredicateEntity<Object>> predicateEntities = SQLExprAnalyzer.analysisPredicate(where, sqlExecutor);
                Map<String, List<PredicateEntity<Object>>> splitPredicateEntities = splitPredicateEntities(predicateEntities);
                leftDfFuture = executorService.submit(() -> {
                    DataSource dataSource = dataSources.get(leftTable.getTName());
                    return dataSource.queryByPredicate(leftTable.getTName(), leftTable.getTAlias(), splitPredicateEntities.get(leftTable.getTAlias()));
                });
                rightDfFuture = executorService.submit(() -> {
                    DataSource dataSource = dataSources.get(rightTable.getTName());
                    return dataSource.queryByPredicate(rightTable.getTName(), rightTable.getTAlias(), splitPredicateEntities.get(rightTable.getTAlias()));
                });

            } else {
                leftDfFuture = executorService.submit(() -> {
                    DataSource dataSource = dataSources.get(leftTable.getTName());
                    return dataSource.queryAll(leftTable.getTName(), leftTable.getTAlias());
                });
                rightDfFuture = executorService.submit(() -> {
                    DataSource dataSource = dataSources.get(rightTable.getTName());
                    return dataSource.queryAll(rightTable.getTName(), rightTable.getTAlias());
                });
            }
            try {
                leftDf = leftDfFuture.get();
                rightDf = rightDfFuture.get();
                cache.put(leftTable.getTName(), leftDf);
                cache.put(rightTable.getTName(), rightDf);
            } catch (InterruptedException | ExecutionException e) {
                log.error("load data failed. ", e);
                return null;
            }

        } else {
            // 只需加载右表
            if (cache.get(rightTable.getTName()) == null) {
                rightDf = dataSources.get(rightTable.getTName()).queryAll(rightTable.getTName(), rightTable.getTAlias());
            } else {
                rightDf = cache.get(rightTable.getTName());
                DataFrameUntil.renameNewTableAlias(rightDf, rightTable.getTAlias());
            }

            leftDf = df;
        }

        //join类型
        DataFrame.JoinType joinType = DataFrameUntil.convertDruiJoinTypeToDFJoinType(joinTypeAndCondation.getJoinType());

        //分析关联字段
        List<PredicateEntity<Object>> conditions = joinTypeAndCondation.getConditions();
        if (conditions.size() == 0) {
            //没有关联条件 笛卡尔积
            log.warn("no on clause find, will use the Cartesian product！！");
            return DataFrameUntil.cartesian(leftDf, rightDf);
        }
        List<Integer> leftOnIndex = new ArrayList<>();
        List<Integer> rightOnIndex = new ArrayList<>();
        Map<String, Integer> leftFiledToIndex = DataFrameUntil.computeFiledToIndex(leftDf);
        Map<String, Integer> rightFieldToIndex = DataFrameUntil.computeFiledToIndex(rightDf);

        for (PredicateEntity<Object> condition : conditions) {
            if (condition.getConnecSymbol() != null && !condition.getConnecSymbol().equals(SQLBinaryOperator.BooleanAnd)) {
                throw new NotSupportException("on condition only support 'AND' operator now,not support 'OR' operator");
            }
            if (condition.getPredicateSymbol() != null && !condition.getPredicateSymbol().equals(SQLBinaryOperator.Equality)) {
                throw new NotSupportException("on condition only support '=' operator now,not support other operator");
            }

            if (condition.getPredicateSymbol() != null) {
                String field = condition.getField();
                String value = (String) condition.getValue();

                if (leftFiledToIndex.get(field) != null && rightFieldToIndex.get(field) != null) {
                    throw new RuntimeException(" Column " + field + " in on clause is ambiguous , please try to use a table alias to solve this problem or is a empty dataframe?");
                }
                if (leftFiledToIndex.get(value) != null && rightFieldToIndex.get(value) != null) {
                    throw new RuntimeException(" Column " + value + " in on clause is ambiguous , please try to use a table alias to solve this problem or is a empty dataframe?");
                }
                if (leftFiledToIndex.get(field) == null && rightFieldToIndex.get(field) == null) {
                    throw new RuntimeException(" Column " + field + " in on clause is not find , please try to use a table alias to solve this problem or is a empty dataframe?");
                }
                if (leftFiledToIndex.get(value) == null && rightFieldToIndex.get(value) == null) {
                    throw new RuntimeException(" Column " + value + " in on clause is not find , please try to use a table alias to solve this problem or is a empty dataframe?");
                }
                if (leftFiledToIndex.get(field) != null) {
                    leftOnIndex.add(leftFiledToIndex.get(field));
                }
                if (leftFiledToIndex.get(value) != null) {
                    leftOnIndex.add(leftFiledToIndex.get(value));
                }
                if (rightFieldToIndex.get(field) != null) {
                    rightOnIndex.add(rightFieldToIndex.get(field));
                }
                if (rightFieldToIndex.get(value) != null) {
                    rightOnIndex.add(rightFieldToIndex.get(value));
                }
            }

        }
        //执行join
        return DataFrameUntil.joinOn(leftDf, rightDf, joinType, leftOnIndex, rightOnIndex);

    }

    /**
     * 按表别名拆分predicateEntities 并去掉括号以及and or，全部使用and连接
     * @param predicateEntities
     * @return
     */
    private Map<String, List<PredicateEntity<Object>>> splitPredicateEntities(List<PredicateEntity<Object>> predicateEntities) {
        final Map<String, List<PredicateEntity<Object>>> map = new HashMap<>();
        predicateEntities.forEach(predicateEntity -> {
            if(predicateEntity.getBrackets() != null){
                return;
            }
            if(predicateEntity.getConnecSymbol()!=null){
                return;
            }
            String field = predicateEntity.getField();
            if(!field.contains(".")){
                throw new RuntimeException("Unable to tell which table the column["+field+"] belongs to,please use tableAlias.column");
            }
            String[] split = field.split("\\.");

            predicateEntity.setField(split[1]);
            if (map.get(split[0])==null) {
                List<PredicateEntity<Object>> predicateEntityList = new ArrayList<>();
                predicateEntityList.add(predicateEntity);
                map.put(split[0],predicateEntityList);
            }else {
                map.get(split[0]).add(predicateEntity);
            }
            map.get(split[0]).add(new PredicateEntity<>(SQLBinaryOperator.BooleanAnd));
        });
        for (Map.Entry<String, List<PredicateEntity<Object>>> entry : map.entrySet()) {
            List<PredicateEntity<Object>> value = entry.getValue();
            value.remove(value.size()-1);
        }

        return map;
    }


}
