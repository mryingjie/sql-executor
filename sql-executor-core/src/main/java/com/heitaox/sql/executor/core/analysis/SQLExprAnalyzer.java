package com.heitaox.sql.executor.core.analysis;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.heitaox.sql.executor.core.entity.JoinTableEntity;
import com.heitaox.sql.executor.core.entity.PredicateEntity;
import com.heitaox.sql.executor.core.util.StringUtil;
import com.heitaox.sql.executor.source.SQLExecutor;
import joinery.DataFrame;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Author ZhengYingjie
 * @Date 2019-08-07
 * @Description
 * Analyze the abstract syntax tree and transform it into the corresponding data structure
 */
@SuppressWarnings("all")
public class SQLExprAnalyzer {


    public static List<PredicateEntity<Object>> analysisPredicate(SQLExpr conditionExpr, SQLExecutor sqlExecutor) {
        List<PredicateEntity<Object>> predicateEntities = new ArrayList<>();
        return analysisPredicate(conditionExpr, predicateEntities, sqlExecutor);
    }

    private static List<PredicateEntity<Object>> analysisPredicate(SQLExpr conditionExpr, List<PredicateEntity<Object>> predicateEntities, SQLExecutor sqlExecutor) {
        if (conditionExpr instanceof SQLBinaryOpExpr) {
            // 不是单个in筛选
            baseAnalysisWhere(predicateEntities, (SQLBinaryOpExpr) conditionExpr, sqlExecutor);
        } else if (conditionExpr instanceof SQLInListExpr) {
            //只有一个in筛选器
            analysisInWhere((SQLInListExpr) conditionExpr, predicateEntities);
        } else if (conditionExpr instanceof SQLInSubQueryExpr) {
            //  where in 类型子查询
            SQLInSubQueryExpr sqlInSubQueryExpr = (SQLInSubQueryExpr) conditionExpr;
            String subSql = sqlInSubQueryExpr.subQuery.toString();
            DataFrame dataFrame = sqlExecutor.executeQuery(subSql);
            if (dataFrame.size() > 1) {
                throw new RuntimeException("error sql: subQuery need one column of data but find more then one");
            }
            List<SQLExpr> sqlExprs = new ArrayList<>();
            for (int i = 0; i < dataFrame.length(); i++) {
                sqlExprs.add(new SQLIdentifierExpr(dataFrame.get(i, 0).toString()));
            }
            SQLExpr expr = ((SQLInSubQueryExpr) conditionExpr).getExpr();
            SQLInListExpr sqlInListExpr = new SQLInListExpr();
            sqlInListExpr.setNot(sqlInSubQueryExpr.isNot());
            sqlInListExpr.setExpr(sqlInSubQueryExpr.getExpr());
            sqlInListExpr.setTargetList(sqlExprs);
            analysisInWhere(sqlInListExpr, predicateEntities);
        }
        return predicateEntities;
    }

    private static List<PredicateEntity<Object>> analysisInWhere(SQLInListExpr inWhere, List<PredicateEntity<Object>> predicateEntities) {
        PredicateEntity<Object> predicateEntity = new PredicateEntity<>();
        List<SQLExpr> targetList = inWhere.getTargetList();
        predicateEntity.setNot(inWhere.isNot());
        predicateEntity.setField(inWhere.getExpr().toString().toLowerCase());
        predicateEntity.setValue(
                targetList.stream().map(str->{
                    if(str instanceof SQLNumericLiteralExpr ){
                        return ((SQLNumericLiteralExpr) str).getNumber();
                    }

                    String s = str.toString();
                    if((s.startsWith("'") && s.endsWith("'")) || (s.startsWith("\"") && s.endsWith("\""))){
                        s = s.substring(1, s.length()-1);
                    }
                    return s;
                }).collect(Collectors.toList())
        );
        predicateEntities.add(predicateEntity);
        return predicateEntities;
    }


    private static List<PredicateEntity<Object>> baseAnalysisWhere(List<PredicateEntity<Object>> predicateEntities, SQLBinaryOpExpr conditionExpr, SQLExecutor sqlExecutor) {
        predicateEntities.add(PredicateEntity.rightBrackets);
        SQLExpr right = conditionExpr.getRight();
        if (right instanceof SQLBinaryOpExpr) {
            baseAnalysisWhere(predicateEntities, (SQLBinaryOpExpr) right, sqlExecutor);
            predicateEntities.add(new PredicateEntity<>(conditionExpr.getOperator()));
            analysisPredicate(conditionExpr.getLeft(), predicateEntities, sqlExecutor);
        } else if (right instanceof SQLInListExpr) {
            SQLInListExpr in = (SQLInListExpr) right;
            analysisInWhere(in, predicateEntities);
            predicateEntities.add(new PredicateEntity<>(conditionExpr.getOperator()));
            analysisPredicate(conditionExpr.getLeft(), predicateEntities, sqlExecutor);
        } else if (right instanceof SQLQueryExpr) {
            // where 子查询
            String subSql = right.toString();
            DataFrame dataFrame = sqlExecutor.executeQuery(subSql);
            if (dataFrame.length() > 1 || dataFrame.size() > 1) {
                throw new RuntimeException("error sql: subQuery need a value but find more then one");
            }

            Object o = dataFrame.get(0, 0);
            // where.setRight();
            conditionExpr.setRight(new SQLIdentifierExpr(o.toString()));
            baseAnalysisWhere(predicateEntities, conditionExpr, sqlExecutor);
        } else if (right instanceof SQLAnyExpr) {
            String subSql = ((SQLAnyExpr) right).getSubQuery().toString();
            DataFrame dataFrame = sqlExecutor.executeQuery(subSql);
            if (dataFrame.size() > 1) {
                throw new RuntimeException("error sql: subQuery need one column of data but find more then one");
            }
            SQLIdentifierExpr subRight = new SQLIdentifierExpr();
            if (dataFrame.length() == 0) {
                subRight.setName(Long.MIN_VALUE + "");
            } else {
                if (conditionExpr.getOperator().equals(SQLBinaryOperator.GreaterThan) || conditionExpr.getOperator().equals(SQLBinaryOperator.GreaterThanOrEqual)) {
                    // 求最小值
                    Object min = Long.MAX_VALUE;
                    for (int i = 0; i < dataFrame.length(); i++) {
                        Object o = dataFrame.get(i, 0);
                        min = StringUtil.compare(o.toString(), min.toString()) > 0 ? min : o;
                    }
                    subRight.setName(min.toString());
                } else {
                    //求最大值
                    Object max = Long.MIN_VALUE;
                    for (int i = 0; i < dataFrame.length(); i++) {
                        Object o = dataFrame.get(i, 0);
                        max = StringUtil.compare(o.toString(), max.toString()) > 0 ? o : max;
                    }
                    subRight.setName(max.toString());
                }
            }
            conditionExpr.setRight(subRight);
            baseAnalysisWhere(predicateEntities, conditionExpr, sqlExecutor);
        } else if (right instanceof SQLAllExpr) {
            String subSql = ((SQLAllExpr) right).getSubQuery().toString();
            DataFrame dataFrame = sqlExecutor.executeQuery(subSql);
            if (dataFrame.size() > 1) {
                throw new RuntimeException("error sql: subQuery need one column of data but find more then one");
            }
            SQLIdentifierExpr subRight = new SQLIdentifierExpr();
            if (dataFrame.length() == 0) {
                subRight.setName(Long.MIN_VALUE + "");
            } else {
                if (conditionExpr.getOperator().equals(SQLBinaryOperator.LessThan) || conditionExpr.getOperator().equals(SQLBinaryOperator.LessThanOrEqual)) {
                    // 求最小值
                    Object min = Long.MAX_VALUE;
                    for (int i = 0; i < dataFrame.length(); i++) {
                        Object o = dataFrame.get(i, 0);
                        min = StringUtil.compare(o.toString(), min.toString()) > 0 ? min : o;
                    }
                    subRight.setName(min.toString());
                } else {
                    //求最大值
                    Object max = Long.MIN_VALUE;
                    for (int i = 0; i < dataFrame.length(); i++) {
                        Object o = dataFrame.get(i, 0);
                        max = StringUtil.compare(o.toString(), max.toString()) > 0 ? o : max;
                    }
                    subRight.setName(max.toString());
                }

            }
            conditionExpr.setRight(subRight);
            baseAnalysisWhere(predicateEntities, conditionExpr, sqlExecutor);
        } else {
            PredicateEntity<Object> entity = new PredicateEntity<>();
            Object value = null;
            if(right instanceof SQLNumericLiteralExpr ){
                value= ((SQLNumericLiteralExpr) right).getNumber();
            }else{
                String s = right.toString();
                if((s.startsWith("'") && s.endsWith("'")) || (s.startsWith("\"") && s.endsWith("\""))){
                    value = s.substring(1, s.length()-1);
                }else {
                    value = s;
                }
            }
            entity.setValue(value);
            entity.setPredicateSymbol(conditionExpr.getOperator());
            entity.setField(conditionExpr.getLeft().toString().toLowerCase());
            predicateEntities.add(entity);
        }
        predicateEntities.add(PredicateEntity.leftBrackets);
        return predicateEntities;
    }

    public static Object[] analysisGroupBy(SQLSelectGroupByClause groupBy, String tableAlias) {
        Object[] groupByIntems = groupBy.getItems().stream().map(item -> {
            if (item instanceof SQLIdentifierExpr) {
                return tableAlias + "." + ((SQLIdentifierExpr) item).getLowerName();
            } else {
                return item.toString().toLowerCase();
            }
        }).toArray();
        return groupByIntems;
    }

    public static List<String> analysisOrderBy(SQLOrderBy orderBy) {
        final List<String> orderByItemStrList = new ArrayList<>();
        List<SQLSelectOrderByItem> items = orderBy.getItems();
        items.forEach(sqlSelectOrderByItem -> {
                    String s = sqlSelectOrderByItem.getExpr().toString();
                    SQLOrderingSpecification type = sqlSelectOrderByItem.getType();
                    s = SQLOrderingSpecification.DESC.equals(type) ? "-" + s : s;
                    orderByItemStrList.add(s);
                }
        );
        return orderByItemStrList;
    }

    public static List<JoinTableEntity> analysisJoinTable(SQLJoinTableSource joinTables) {
        List<JoinTableEntity> joinTableEntities = new ArrayList<>();
        baseanalysisJoinTable(joinTables, joinTableEntities);
        return joinTableEntities;
    }

    private static void baseanalysisJoinTable(SQLTableSource joinTables, List<JoinTableEntity> joinTableEntities) {
        if (joinTables instanceof SQLExprTableSource) {
            String tableName = ((SQLExprTableSource) joinTables).getExpr().toString();
            joinTableEntities.add(new JoinTableEntity(joinTables.getAlias() == null ? tableName : joinTables.getAlias(), tableName));
        } else {
            SQLJoinTableSource joinTableSource = (SQLJoinTableSource) joinTables;

            SQLExprTableSource right = (SQLExprTableSource) joinTableSource.getRight();
            baseanalysisJoinTable(right, joinTableEntities);

            SQLJoinTableSource.JoinType joinType = joinTableSource.getJoinType();
            SQLBinaryOpExpr condition = (SQLBinaryOpExpr) joinTableSource.getCondition();
            List<PredicateEntity<Object>> predicateEntities = analysisJoinCondition(condition);
            joinTableEntities.add(new JoinTableEntity(joinType, predicateEntities));


            SQLTableSource left = joinTableSource.getLeft();
            baseanalysisJoinTable(left, joinTableEntities);
        }

    }

    private static List<PredicateEntity<Object>> analysisJoinCondition(SQLBinaryOpExpr condition) {
        List<PredicateEntity<Object>> predicateEntities = new ArrayList<>();
        analysisPredicate(condition, predicateEntities, null);

        return predicateEntities;
    }

    public static List<Map<String, Object>> analysisInsert(SQLInsertStatement insertStatement) {
        final List<String> columns = insertStatement.getColumns()
                .stream()
                .map(Object::toString)
                .collect(Collectors.toList());
        List<Map<String, Object>> valueList = insertStatement.getValuesList()
                .stream()
                .map(valuesClause -> {
                    List<SQLExpr> values = valuesClause.getValues();
                    Map<String, Object> valueMap = new HashMap<>();

                    for (int i = 0; i < values.size(); i++) {
                        SQLExpr sqlExpr = values.get(i);
                        Object v;
                        if(sqlExpr instanceof SQLNumericLiteralExpr ){
                            v = ((SQLNumericLiteralExpr) sqlExpr).getNumber();
                        }else if(sqlExpr instanceof SQLCharExpr){
                            String s = sqlExpr.toString();
                            v =  s.substring(1, s.length()-1);
                        }else {
                            v = sqlExpr.toString();
                        }

                        valueMap.put(columns.get(i), v);
                    }
                    return valueMap;
                }).collect(Collectors.toList());
        return valueList;
    }

    public static Map<String, Object> analysisUpdateItem(List<SQLUpdateSetItem> items) {
        Map<String,Object> updateItems = new HashMap<>(items.size());
        for (SQLUpdateSetItem item : items) {
            SQLExpr column = item.getColumn();
            SQLExpr value = item.getValue();
            Object v;
            if(value instanceof SQLNumericLiteralExpr ){
                v = ((SQLNumericLiteralExpr) value).getNumber();
            }else if(value instanceof SQLCharExpr){
                String s = value.toString();
                v =  s.substring(1, s.length()-1);
            }else {
                v = value.toString();
            }
            updateItems.put(column.toString(), v);

        }
        return updateItems;
    }
}
