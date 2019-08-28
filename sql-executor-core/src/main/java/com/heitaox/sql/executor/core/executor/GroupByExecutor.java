package com.heitaox.sql.executor.core.executor;


import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.heitaox.sql.executor.core.analysis.SQLExprAnalyzer;
import com.heitaox.sql.executor.core.entity.PredicateEntity;
import com.heitaox.sql.executor.core.function.udaf.COUNT;
import com.heitaox.sql.executor.core.function.udaf.UDAF;
import com.heitaox.sql.executor.core.function.udf.UDF;
import com.heitaox.sql.executor.core.function.udf2.UDF2;
import com.heitaox.sql.executor.core.function.udf3.UDF3;
import com.heitaox.sql.executor.core.util.ClassConvertUtil;
import com.heitaox.sql.executor.core.util.DataFrameUntil;
import joinery.DataFrame;
import joinery.impl.Grouping;
import joinery.impl.SparseBitSet;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.function.BiConsumer;

@Slf4j
@AllArgsConstructor
public class GroupByExecutor extends BaseExecutor {

    private SQLSelectGroupByClause groupBy;

    private List<SQLSelectItem> selectList;

    private Map<String, Integer> columnToIndex;

    private String tableAlias;

    private Map<String, Class> funcMap;

    @Override
    public DataFrame execute(DataFrame df) {
        Map<List<Object>, List<Object>> keysToDataList;

        Object[] groupByItems = SQLExprAnalyzer.analysisGroupBy(groupBy, tableAlias);
        if (groupByItems.length != 0) {
            keysToDataList = excuteGroupBy(df, groupByItems);
            // 三、执行函数 获取查询的列名
            df = new FunctionExecutor().execute(keysToDataList, true);

        } else {
            //只有子句having
            keysToDataList = DataFrameUntil.dfToMapList(df);
            // 三、执行函数 获取查询的列名
            df = new FunctionExecutor().execute(keysToDataList, false);
        }
        //更新字段名到数据的映射
        columnToIndex = DataFrameUntil.computeFiledToIndex(df);
        //四 、 执行having 再次进行筛选
        SQLExpr having = groupBy.getHaving();
        if (having != null) {
            df = new PredicateFilter(having, columnToIndex, tableAlias, null).execute(df);
        }
        return df;
    }

    private Map<List<Object>, List<Object>> excuteGroupBy(DataFrame df, Object[] groupByItems) {
        final DataFrame groupDf = df.groupBy(groupByItems);
        Grouping groups = groupDf.groups();
        Map<List<Object>, List<Object>> keysToDataGroup = new HashMap<>(groupByItems.length);
        Set columns = groupDf.columns();
        Iterator<Map.Entry<Object, SparseBitSet>> iterator = groups.iterator();
        iterator.forEachRemaining(objectSparseBitSetEntry -> {
            Object key = objectSparseBitSetEntry.getKey();
            SparseBitSet value = objectSparseBitSetEntry.getValue();
            List<Integer> indexs = new ArrayList<>();
            for (int i = value.nextSetBit(0); i >= 0; i = value.nextSetBit(i + 1)) {
                indexs.add(i);
            }
            List<Object> keys = new ArrayList<>();
            if (key instanceof Iterable) {
                Iterable cKey = (Iterable) key;
                for (Object o : cKey) {
                    keys.add(o);
                }
            } else {
                keys.add(key);
            }
            List<Object> groupList = new ArrayList<>(indexs.size());
            for (Integer row : indexs) {

                List<Object> columList = new ArrayList<>(columns.size());
                for (Object colum : columns) {
                    columList.add(groupDf.get(row, colum));
                }
                groupList.add(columList);
            }
            keysToDataGroup.put(keys, groupList);
        });
        return keysToDataGroup;
    }


    public class FunctionExecutor {

        public Object executeFunc(SQLExpr expr, List<Object> values) {
            Object res = null;
            if (expr instanceof SQLMethodInvokeExpr) {
                // UDF函数
                Class aClass = funcMap.get(((SQLMethodInvokeExpr) expr).getMethodName().toLowerCase());
                if (aClass == null) {
                    throw new RuntimeException("can not find the method with name[" + ((SQLMethodInvokeExpr) expr).getMethodName() + "]");
                }
                List<SQLExpr> parameters = ((SQLMethodInvokeExpr) expr).getParameters();
                if (parameters.size() == 1) {
                    //udf
                    Class udfClass = aClass;
                    SQLExpr sqlExpr = parameters.get(0);
                    Object param = executeFunc(sqlExpr, values);
                    try {
                        Object o = udfClass.newInstance();
                        Type type = udfClass.getGenericSuperclass();
                        //转换为泛型
                        ParameterizedType pt = (ParameterizedType) type;
                        // 获取参数化类型中，实际类型的定义
                        Type[] ts = pt.getActualTypeArguments();
                        Class inputClass = (Class) ts[0];
                        Class outPutClass = (Class) ts[1];
                        Method transMethod = udfClass.getMethod(UDF.TRANS_METHOD, inputClass);
                        Object invokeRes = transMethod.invoke(o, ClassConvertUtil.convertClass(inputClass, param));
                        return ClassConvertUtil.convertClass(outPutClass, invokeRes);
                    } catch (InstantiationException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }

                }
                if (parameters.size() == 2) {
                    //ud2f
                    Class udf2Class = aClass;
                    List<Object> params = new ArrayList<>(2);
                    for (SQLExpr parameter : parameters) {
                        params.add(executeFunc(parameter, values));
                    }
                    try {
                        Object o = udf2Class.newInstance();
                        Type type = udf2Class.getGenericSuperclass();
                        //转换为泛型
                        ParameterizedType pt = (ParameterizedType) type;
                        // 获取参数化类型中，实际类型的定义
                        Type[] ts = pt.getActualTypeArguments();
                        Class inputClass1 = (Class) ts[0];
                        Class inputClass2 = (Class) ts[1];
                        Class outPutClass = (Class) ts[2];
                        Method method = udf2Class.getMethod(UDF2.TRANS_METHOD, inputClass1, inputClass2);
                        res = method.invoke(
                                o,
                                ClassConvertUtil.convertClass(inputClass1, params.get(0)),
                                ClassConvertUtil.convertClass(inputClass2, params.get(1))
                        );

                        return ClassConvertUtil.convertClass(outPutClass, res);
                    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
                if (parameters.size() == 3) {
                    //udf3
                    Class udf2Class = aClass;
                    List<Object> params = new ArrayList<>(2);
                    for (SQLExpr parameter : parameters) {
                        params.add(executeFunc(parameter, values));
                    }
                    try {
                        Object o = udf2Class.newInstance();
                        Type type = udf2Class.getGenericSuperclass();
                        //转换为泛型
                        ParameterizedType pt = (ParameterizedType) type;
                        // 获取参数化类型中，实际类型的定义
                        Type[] ts = pt.getActualTypeArguments();
                        Class inputClass1 = (Class) ts[0];
                        Class inputClass2 = (Class) ts[1];
                        Class inputClass3 = (Class) ts[2];
                        Class outPutClass = (Class) ts[3];
                        Method method = udf2Class.getMethod(UDF3.TRANS_METHOD, inputClass1, inputClass2, inputClass3);
                        res = method.invoke(
                                o,
                                ClassConvertUtil.convertClass(inputClass1, params.get(0)),
                                ClassConvertUtil.convertClass(inputClass2, params.get(1)),
                                ClassConvertUtil.convertClass(inputClass3, params.get(2))
                        );

                        return ClassConvertUtil.convertClass(outPutClass, res);
                    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }

            } else if (expr instanceof SQLAggregateExpr) {
                // UDAF函数
                Class aClass = funcMap.get(((SQLAggregateExpr) expr).getMethodName().toLowerCase());
                if (aClass == null) {
                    throw new RuntimeException("can not find the method with name[" + ((SQLAggregateExpr) expr).getMethodName() + "]");
                }
                List<SQLExpr> arguments = ((SQLAggregateExpr) expr).getArguments();

                List<Object> params = new ArrayList<>(values.size());
                for (SQLExpr argument : arguments) {
                    if (COUNT.class.equals(aClass) && argument instanceof SQLAllColumnExpr) {
                        return values.size();
                    }

                    List<Object> objects = new ArrayList<>(1);
                    for (Object value : values) {
                        objects.add(value);
                        params.add(executeFunc(argument, objects));
                        objects.clear();
                    }
                }
                try {
                    Class udafClass = aClass;
                    Object o = udafClass.newInstance();
                    Type type = udafClass.getGenericSuperclass();
                    //转换为泛型
                    ParameterizedType pt = (ParameterizedType) type;
                    // 获取参数化类型中，实际类型的定义
                    Type[] ts = pt.getActualTypeArguments();
                    Class inputClass = (Class) ts[0];
                    Class outPutClass = (Class) ts[1];
                    Method initMethod = udafClass.getMethod(UDAF.INIT_MATHOD_NAME);
                    Method aggreateMethod = udafClass.getMethod(UDAF.AGGREGATE_METHOD_NAME, inputClass, outPutClass);
                    Method computeResultMethod = udafClass.getMethod(UDAF.COMPUTE_RESULT_METHOD_NAME, outPutClass);
                    res = initMethod.invoke(o, null);
                    for (Object param : params) {
                        res = aggreateMethod.invoke(o, ClassConvertUtil.convertClass(inputClass, param), res);
                    }
                    return computeResultMethod.invoke(o, res);
                } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            } else if (expr instanceof SQLIdentifierExpr) {
                List value = (List) values.get(0);
                Integer index = columnToIndex.get((tableAlias + "." + expr.toString()).toLowerCase());
                if (index == null) {
                    throw new RuntimeException("can not find this column [" + expr + "]");
                }
                return value.get(index);
            } else if (expr instanceof SQLPropertyExpr) {
                List value = (List) values.get(0);
                //普通列名 有表别名
                Integer index = columnToIndex.get(expr.toString().toLowerCase());
                if (index == null) {
                    throw new RuntimeException("can not find this column [" + expr + "]");
                }
                return value.get(index);
            } else if (expr instanceof SQLValuableExpr) {
                // 常量
                String valueStr = expr.toString();
                if ((valueStr.startsWith("'") && valueStr.endsWith("'")) || (valueStr.startsWith("\"") && valueStr.endsWith("\""))) {
                    valueStr = valueStr.substring(1, valueStr.length() - 1);
                }
                return valueStr;
            } else if (expr instanceof SQLAllColumnExpr) {
                return "*";
            } else if (expr instanceof SQLBinaryOpExpr) {
                List<PredicateEntity<Object>> predicateEntities = SQLExprAnalyzer.analysisPredicate(expr, null);
                return DataFrameUntil.exectuePredicaeEntity(predicateEntities, columnToIndex, (List<Object>) values.get(0), tableAlias);
            } else if (expr instanceof SQLCaseExpr) {

                SQLExpr valueExpr = ((SQLCaseExpr) expr).getValueExpr();
                List<SQLCaseExpr.Item> items = ((SQLCaseExpr) expr).getItems();

                Object caseValue = null;
                if (valueExpr != null) {
                    caseValue = executeFunc(valueExpr, values);
                    for (SQLCaseExpr.Item item : items) {
                        SQLExpr conditionExpr = item.getConditionExpr();
                        Object whenValue = executeFunc(conditionExpr, values);
                        if (caseValue.equals(whenValue)) {
                            return executeFunc(item.getValueExpr(), values);
                        }
                    }

                } else {
                    for (SQLCaseExpr.Item item : items) {
                        SQLExpr conditionExpr = item.getConditionExpr();
                        List<PredicateEntity<Object>> predicateEntities = SQLExprAnalyzer.analysisPredicate(conditionExpr, null);
                        if(DataFrameUntil.exectuePredicaeEntity(predicateEntities, columnToIndex, (List<Object>) values.get(0), tableAlias)){
                            return executeFunc(item.getValueExpr(), values);
                        }
                    }

                }
                SQLExpr elseExpr = ((SQLCaseExpr) expr).getElseExpr();
                if (elseExpr != null) {
                    return executeFunc(elseExpr, values);
                }
            }
            return res;
        }

        /**
         * 这个方法实际是在准备执行函数的所需的数据  真正的执行逻辑在executeFunc方法
         *
         * @param keysToDataList groupBy聚合后的数据
         * @param haveGroup 是否有group
         * @return DataFrame
         */
        public DataFrame execute(Map<List<Object>, List<Object>> keysToDataList, boolean haveGroup) {
            DataFrame dataFrame = new DataFrame();
            //6 所有常量
            Map<String, Object> constantList = new HashMap<>();
            Collection<List<Object>> groupValues = keysToDataList.values();
            for (SQLSelectItem sqlSelectItem : selectList) {
                List<Object> columnValue = new ArrayList<>(groupValues.size());
                SQLExpr expr = sqlSelectItem.getExpr();
                String alias = sqlSelectItem.getAlias() == null ? expr.toString() : sqlSelectItem.getAlias();
                if (expr instanceof SQLIdentifierExpr) {
                    //普通列名 没有表别名
                    Integer index = columnToIndex.get((tableAlias + "." + expr.toString()).toLowerCase());
                    if (haveGroup) {
                        for (List<Object> values : groupValues) {
                            List value = (List) values.get(0);
                            columnValue.add(value.get(index));
                        }
                        dataFrame.add(alias, columnValue);

                    } else {
                        for (List<Object> values : groupValues) {
                            for (Object value : values) {
                                columnValue.add(((List) value).get(index));
                            }
                            dataFrame.add(alias, columnValue);
                        }
                    }


                } else if (expr instanceof SQLPropertyExpr) {
                    //普通列名 有表别名
                    Integer index = columnToIndex.get(expr.toString().toLowerCase());
                    if (haveGroup) {
                        for (List<Object> values : groupValues) {
                            List value = (List) values.get(0);
                            columnValue.add(value.get(index));
                        }
                    } else {
                        for (List<Object> values : groupValues) {
                            for (Object value : values) {
                                columnValue.add(((List) value).get(index));
                            }
                        }
                    }
                    dataFrame.add(alias, columnValue);
                } else if (expr instanceof SQLMethodInvokeExpr) {
                    // UDF函数
                    if (haveGroup) {
                        for (List<Object> values : groupValues) {
                            Object res = executeFunc(expr, values);
                            columnValue.add(res);
                        }
                    } else {
                        for (List<Object> values : groupValues) {
                            List<Object> objects = new ArrayList<>(1);
                            for (Object value : values) {
                                objects.add(value);
                                Object res = executeFunc(expr, objects);
                                columnValue.add(res);
                                objects.clear();
                            }
                        }
                    }
                    if (dataFrame.length() != 0 && dataFrame.length() != columnValue.size()) {
                        throw new RuntimeException("sql error: udaf function and (udf function or simple column) cannot appear at the same timet ");
                    }
                    dataFrame.add(alias, columnValue);


                } else if (expr instanceof SQLAggregateExpr) {
                    // UDAF函数

                    for (List<Object> values : groupValues) {
                        Object res = executeFunc(expr, values);
                        columnValue.add(res);
                    }
                    if (dataFrame.length() != 0 && dataFrame.length() != columnValue.size()) {
                        throw new RuntimeException("sql error: udaf function and (udf function or simple column) cannot appear at the same timet ");
                    }

                    dataFrame.add(alias, columnValue);

                } else if (expr instanceof SQLValuableExpr) {
                    // 常量 最后处理

                    if ((alias.startsWith("'") && alias.endsWith("'")) || (alias.startsWith("\"") && alias.endsWith("\""))) {
                        alias = alias.substring(1, alias.length() - 1);
                    }
                    constantList.put(alias, ((SQLValuableExpr) expr).getValue());

                } else if (expr instanceof SQLAllColumnExpr) {
                    Set<String> keys = columnToIndex.keySet();
                    if (haveGroup) {
                        for (String key : keys) {
                            List<Object> objects = new ArrayList<>();
                            Integer index = columnToIndex.get(key.toLowerCase());
                            for (List<Object> values : groupValues) {
                                List value = (List) values.get(0);
                                objects.add(value.get(index));
                            }
                            dataFrame.add(key, objects);
                        }
                    } else {
                        for (String key : keys) {
                            List<Object> objects = new ArrayList<>();
                            Integer index = columnToIndex.get(key.toLowerCase());
                            for (List<Object> values : groupValues) {
                                for (Object value : values) {
                                    objects.add(((List) value).get(index));
                                }
                            }
                            dataFrame.add(key, objects);
                        }
                    }
                } else if (expr instanceof SQLCaseExpr) {
                    if (haveGroup) {
                        for (List<Object> values : groupValues) {
                            Object res = executeFunc(expr, values);
                            columnValue.add(res);
                        }
                    } else {
                        for (List<Object> values : groupValues) {
                            List<Object> objects = new ArrayList<>(1);
                            for (Object value : values) {
                                objects.add(value);
                                Object res = executeFunc(expr, objects);
                                columnValue.add(res);
                                objects.clear();
                            }
                        }
                    }
                }

            }
            //处理常量
            if (constantList.size() > 0) {

                constantList.forEach(new BiConsumer<String, Object>() {
                    @Override
                    public void accept(String alias, Object value) {
                        if (dataFrame.length() == 0) {
                            dataFrame.add(alias, Arrays.asList(value));
                            return;
                        }
                        List<Object> values = new ArrayList<>(dataFrame.length());
                        for (int i = 0; i < dataFrame.length(); i++) {
                            values.add(value);
                        }
                        dataFrame.add(alias, values);
                    }
                });
            }

            return dataFrame;
        }

    }

}
