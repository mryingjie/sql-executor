package com.heitaox.sql.executor.core.util;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionOperator;
import com.heitaox.sql.executor.core.entity.PredicateEntity;
import com.heitaox.sql.executor.core.exception.NotSupportException;
import joinery.DataFrame;
import lombok.extern.slf4j.Slf4j;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.*;

@SuppressWarnings("all")
@Slf4j
public class DataFrameUntil {

    private static final ScriptEngine scriptEngine = new ScriptEngineManager().getEngineByName("js");


    public static Map<String, Integer> computeFiledToIndex(DataFrame df) {
        Set columns = df.columns();
        Map<String, Integer> columnToIndex = new LinkedHashMap<>(columns.size());
        int i = 0;
        // 字段名到值的映射
        for (Object column : columns) {
            columnToIndex.put(column.toString().toLowerCase(), i);
            i++;
        }
        return columnToIndex;
    }


    public static void dropColumnTableAlias(DataFrame<Object> df) {
        Set<Object> columns = df.columns();
        List<Object> objects = new ArrayList<>(columns);
        for (Object column : objects) {
            String columnName = (String) column;
            if (columnName.contains(".")) {
                columnName = columnName.split("\\.")[1];
                df.rename(column, columnName);
            }
        }
    }

    public static void setColumnTableAlias(DataFrame<Object> df, String alilas) {
        Set<Object> columns = df.columns();
        List<Object> objects = new ArrayList<>(columns);
        for (Object column : objects) {
            String columnName = (String) column;
            if (!columnName.contains(".")) {
                columnName = alilas + "." + columnName;
                df.rename(column, columnName);
            }
        }
    }

    public static void renameNewTableAlias(DataFrame df, String tableAlias) {
        Set columns = df.columns();
        List<Object> objects = new ArrayList<>(columns);
        for (Object column : objects) {
            String columnName = (String) column;
            if (columnName.contains(".")) {
                columnName = tableAlias + "." + columnName.split("\\.")[1];
                df.rename(column, columnName);
            } else {
                columnName = tableAlias + "." + columnName;
                df.rename(column, columnName);
            }
        }
    }

    public static DataFrame.JoinType convertDruiJoinTypeToDFJoinType(SQLJoinTableSource.JoinType joinType) {
        DataFrame.JoinType dfJoinType;

        if (SQLJoinTableSource.JoinType.INNER_JOIN.equals(joinType)
                || SQLJoinTableSource.JoinType.JOIN.equals(joinType)
                || SQLJoinTableSource.JoinType.STRAIGHT_JOIN.equals(joinType)) {
            dfJoinType = DataFrame.JoinType.INNER;
        } else if (SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN.equals(joinType)) {
            dfJoinType = DataFrame.JoinType.LEFT;
        } else if (SQLJoinTableSource.JoinType.RIGHT_OUTER_JOIN.equals(joinType)) {
            dfJoinType = DataFrame.JoinType.RIGHT;
        } else if (SQLJoinTableSource.JoinType.FULL_OUTER_JOIN.equals(joinType)) {
            dfJoinType = DataFrame.JoinType.OUTER;
        } else {
            throw new NotSupportException("not support this joinType of:" + joinType);
        }
        return dfJoinType;
    }

    public static Map<List<Object>, List<Object>> dfToMapList(DataFrame df) {
        Map<List<Object>, List<Object>> keysToDataGroup = new HashMap<>(df.length());
        List<Object> rowList = new ArrayList<>(df.length());//行
        for (int i = 0; i < df.length(); i++) {
            List row = df.row(i);
            rowList.add(row);
        }
        keysToDataGroup.put(null, rowList);
        return keysToDataGroup;
    }

    public static DataFrame sortBy(DataFrame df, Map<String, Integer> columnToIndex, final Object... cols) {
        final Map<Integer, DataFrame.SortDirection> sortCols = new LinkedHashMap<>();
        for (final Object col : cols) {
            final String str = col instanceof String ? String.class.cast(col) : "";
            final DataFrame.SortDirection dir = str.startsWith("-") ?
                    DataFrame.SortDirection.DESCENDING : DataFrame.SortDirection.ASCENDING;

            final int c = columnToIndex.get((str.startsWith("-") ? str.substring(1) : col).toString().toLowerCase());
            sortCols.put(c, dir);
        }
        return Sorting.sort(df, sortCols);

    }

    public static DataFrame<Object> union(DataFrame<Object> leftDf, DataFrame<Object> rightDf, SQLUnionOperator operator,Map<String, Integer> columnToIndex) {
        DataFrame dataFrame = new DataFrame();
        for (Object column : leftDf.columns()) {
            dataFrame.add(column);
        }
        for (List<Object> row : rightDf) {
            leftDf.append(row);
        }
        if(SQLUnionOperator.UNION.equals(operator)){
            //按某个字段排序
            sortBy(leftDf,columnToIndex,new ArrayList<>(columnToIndex.keySet()).get(0) );
            List<Object> cacheRow = null;
            for (List<Object> row : leftDf) {
                if(!row.equals(cacheRow)){
                    dataFrame.append(row);
                    cacheRow = row;
                }
            }
        }else if(SQLUnionOperator.UNION_ALL.equals(operator)){
            dataFrame = leftDf;

        }else{
            throw new NotSupportException("SQLUnionOperator only support 'union' and 'union all' , not support SQLUnionOperator of '"+operator.toString()+"'");
        }
        return dataFrame;
    }


    public static class Sorting {
        public static <V> DataFrame<V> sort(
                final DataFrame<V> df, final Map<Integer, DataFrame.SortDirection> cols) {
            final Comparator<List<V>> comparator = new Comparator<List<V>>() {
                @Override
                public int compare(final List<V> r1, final List<V> r2) {
                    int result = 0;
                    for (final Map.Entry<Integer, DataFrame.SortDirection> col : cols.entrySet()) {
                        final int c = col.getKey();
                        V v = r1.get(c);
                        if (v != null) {
                            final Comparable<V> v1 = Comparable.class.cast(v);
                            final V v2 = r2.get(c);
                            if (v2 != null) {
                                result = v1.compareTo(v2);
                            } else {
                                result = 1;
                            }
                            result *= col.getValue() == DataFrame.SortDirection.DESCENDING ? -1 : 1;

                        } else {
                            final V v2 = r2.get(c);
                            if (v2 != null) {
                                result = -1;
                            }else {
                                result = 0;
                            }
                            result *= col.getValue() == DataFrame.SortDirection.DESCENDING ? -1 : 1;
                        }
                        if (result != 0) {
                            break;
                        }
                    }
                    return result;
                }
            };
            return sort(df, comparator);
        }

        public static <V> DataFrame<V> sort(
                final DataFrame<V> df, final Comparator<List<V>> comparator) {
            final DataFrame<V> sorted = new DataFrame<V>(df.columns());
            final Comparator<Integer> cmp = new Comparator<Integer>() {
                @Override
                public int compare(final Integer r1, final Integer r2) {
                    return comparator.compare(df.row(r1), df.row(r2));
                }
            };

            final Integer[] rows = new Integer[df.length()];
            for (int r = 0; r < df.length(); r++) {
                rows[r] = r;
            }
            Arrays.sort(rows, cmp);

            final List<Object> labels = new ArrayList<>(df.index());
            for (final Integer r : rows) {
                final Object label = r < labels.size() ? labels.get(r) : r;
                sorted.append(label, df.row(r));
            }

            return sorted;
        }
    }


    public static <V> DataFrame<V> join(final DataFrame<V> left, final DataFrame<V> right, final DataFrame.JoinType how, final DataFrame.KeyFunction<V> leftOn, final DataFrame.KeyFunction<V> rightOn) {

        final List<Map<Object, List<V>>> keyToRowMapListLeft = new ArrayList<>();
        final List<Map<Object, List<V>>> keyToRowMapListRight = new ArrayList<>();

        //压缩数据
        transDFToRowMapList(left, leftOn, keyToRowMapListLeft);
        transDFToRowMapList(right, rightOn, keyToRowMapListRight);

        List<Object> joinColumns = new ArrayList<>(left.columns());
        joinColumns.addAll(right.columns());
        DataFrame<V> joinDf = new DataFrame<>(joinColumns);

        if (how == DataFrame.JoinType.INNER) {
            for (Map<Object, List<V>> keyToRowMapLeft : keyToRowMapListLeft) {
                Set<Object> keys = keyToRowMapLeft.keySet();
                for (Object key : keys) {
                    List<V> leftRow = keyToRowMapLeft.get(key);
                    List<V> rightRow = null;
                    for (Map<Object, List<V>> keyToRowMapRight : keyToRowMapListRight) {
                        rightRow = keyToRowMapRight.get(key);
                        if (rightRow != null) {
                            List<V> row = new ArrayList<>(leftRow);
                            row.addAll(rightRow);
                            joinDf.append(row);
                        }
                    }
                }

            }
        } else if (how == DataFrame.JoinType.LEFT) {
            for (Map<Object, List<V>> keyToRowMapLeft : keyToRowMapListLeft) {
                Set<Object> keys = keyToRowMapLeft.keySet();
                for (Object key : keys) {
                    List<V> leftRow = keyToRowMapLeft.get(key);
                    List<V> rightRow = null;
                    List<V> row = new ArrayList<>(leftRow);
                    boolean match = false;
                    for (Map<Object, List<V>> keyToRowMapRight : keyToRowMapListRight) {
                        rightRow = keyToRowMapRight.get(key);
                        if (rightRow != null) {
                            match = true;
                            ArrayList<V> row1 = new ArrayList<>(leftRow);
                            row1.addAll(rightRow);
                            joinDf.append(row1);
                        }
                    }
                    if (!match) {
                        joinDf.append(row);
                    }
                }

            }

        } else if (how == DataFrame.JoinType.RIGHT) {
            for (Map<Object, List<V>> keyToRowMapRight : keyToRowMapListRight) {
                Set<Object> keys = keyToRowMapRight.keySet();
                for (Object key : keys) {
                    List<V> rightRow = keyToRowMapRight.get(key);
                    List<V> leftRow = null;
                    List<V> row = new ArrayList<>(rightRow);
                    boolean match = false;
                    for (Map<Object, List<V>> keyToRowMapLeft : keyToRowMapListLeft) {
                        leftRow = keyToRowMapLeft.get(key);
                        if (leftRow != null) {
                            match = true;
                            ArrayList<V> row1 = new ArrayList<>(rightRow);
                            row1.addAll(leftRow);
                            joinDf.append(row1);
                        }
                    }
                    if (!match) {
                        joinDf.append(row);
                    }
                }

            }
        } else {
            //OUTER
            for (Map<Object, List<V>> keyToRowMapLeft : keyToRowMapListLeft) {
                Set<Object> keys = keyToRowMapLeft.keySet();
                for (Object key : keys) {
                    List<V> leftRow = keyToRowMapLeft.get(key);
                    List<V> rightRow = null;
                    List<V> row = new ArrayList<>(leftRow);
                    boolean match = false;
                    for (Map<Object, List<V>> keyToRowMapRight : keyToRowMapListRight) {
                        rightRow = keyToRowMapRight.get(key);
                        if (rightRow != null) {
                            match = true;
                            ArrayList<V> row1 = new ArrayList<>(leftRow);
                            row1.addAll(rightRow);
                            joinDf.append(row1);
                        }
                    }
                    if (!match) {
                        joinDf.append(row);
                    }
                }

            }
            for (Map<Object, List<V>> keyToRowMapRight : keyToRowMapListRight) {
                Set<Object> keys = keyToRowMapRight.keySet();
                for (Object key : keys) {
                    List<V> rightRow = keyToRowMapRight.get(key);
                    List<V> leftRow = null;
                    boolean match = false;
                    for (Map<Object, List<V>> keyToRowMapLeft : keyToRowMapListLeft) {
                        leftRow = keyToRowMapLeft.get(key);
                        if (leftRow != null) {
                            match = true;
                            break;
                        }
                    }
                    if (!match) {
                        int leftSize = keyToRowMapListLeft.get(0).values().size();
                        leftRow = new ArrayList<>(leftSize);
                        for (int i = 0; i < leftSize; i++) {
                            leftRow.add(null);
                        }
                        List<V> row = new ArrayList<>(leftRow);
                        row.addAll(rightRow);
                        joinDf.append(row);
                    }
                }

            }

        }

        return joinDf;
    }

    private static <V> void transDFToRowMapList(DataFrame<V> left, DataFrame.KeyFunction<V> leftOn, List<Map<Object, List<V>>> keyToRowMapList) {
        for (List<V> row : left) {
            Object key = leftOn.apply(row);
            if (keyToRowMapList.size() == 0) {
                keyToRowMapList.add(new HashMap<>());
            }
            boolean flag = false;
            for (Map<Object, List<V>> keyToRowMap : keyToRowMapList) {
                if (keyToRowMap.get(key) == null) {
                    keyToRowMap.put(key, row);
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                Map<Object, List<V>> keyToRowMap = new HashMap<>();
                keyToRowMap.put(key, row);
                keyToRowMapList.add(keyToRowMap);
            }
        }
    }


    public static <V> DataFrame<V> joinOn(final DataFrame<V> left, final DataFrame<V> right, final DataFrame.JoinType how, final List<Integer> leftCols, final List<Integer> rightCols) {
        return join(
                left,
                right,
                how,
                value -> {
                    final List<V> key = new ArrayList<>(leftCols.size());
                    for (final int col : leftCols) {
                        key.add(value.get(col));
                    }
                    return Collections.unmodifiableList(key);
                },
                value -> {
                    final List<V> key = new ArrayList<>(rightCols.size());
                    for (final int col : rightCols) {
                        key.add(value.get(col));
                    }
                    return Collections.unmodifiableList(key);
                });
    }

    public static <V> DataFrame<V> cartesian(DataFrame<V> leftDf, DataFrame<V> rightDf) {
        Set leftColumns = leftDf.columns();
        List columns = new ArrayList<>(leftColumns);
        Set rightColumns = rightDf.columns();
        for (Object rightColumn : rightColumns) {
            columns.add(rightColumn);
        }
        DataFrame<V> dataFrame = new DataFrame<>();
        dataFrame.add(columns.toArray());
        for (List<V> leftRow : leftDf) {
            for (List<V> rightRow : rightDf) {
                List<V> row = new ArrayList<>(leftRow);
                row.addAll(rightRow);
                dataFrame.append(row);
            }
        }
        return dataFrame;
    }
    public static DataFrame filter(List<PredicateEntity<Object>> predicateEntities, DataFrame<Object> df, Map<String, Integer> columnToIndex, String tableAlias) {
        DataFrame<Object> dataFrame = new DataFrame<>(Arrays.asList(df.columns().toArray()));
        for (List<Object> values : df) {
            if(DataFrameUntil.exectuePredicaeEntity(predicateEntities, columnToIndex, values, tableAlias)){
                dataFrame.append(values);
            }
        }
        return dataFrame;
    }

    public static Boolean exectuePredicaeEntity(List<PredicateEntity<Object>> predicateEntities, Map<String, Integer> columnToIndex, List<Object> values,String tableAlias) {
        if(predicateEntities==null|| predicateEntities.size()==0){
            return true;
        }
        // 最终组成的逻辑字符串
        StringBuilder str = new StringBuilder();
        for (int i1 = predicateEntities.size() - 1; i1 >= 0; i1--) {
            PredicateEntity<Object> entity = predicateEntities.get(i1);
            // 判断是不是括号
            if (entity.getBrackets() != null) {
                str.append(entity.getBrackets());
                continue;
            }
            //判断是不是and 和 or
            if (entity.getConnecSymbol() != null) {
                if (SQLBinaryOperator.BooleanAnd.equals(entity.getConnecSymbol())) {
                    str.append("&&");
                } else {
                    str.append("||");
                }
                continue;
            }
            boolean predicte = true;
            String field = entity.getField();
            Object sqlValue = entity.getValue();
            Integer columnIndex = columnToIndex.get(field.toLowerCase()) == null?columnToIndex.get((tableAlias+"."+field).toLowerCase()):columnToIndex.get(field.toLowerCase());
            Object dataValue = values.get(columnIndex);
            if(dataValue == null){
                dataValue = "";
            }
            //首先判断是不是in筛选器
            if (sqlValue instanceof List) {
                List sqlValues = (List) sqlValue;
                Boolean not = entity.getNot();
                if (not) {
                    // not in
                    predicte = !sqlValues.contains(dataValue.toString());
                } else {
                    // in
                    predicte = sqlValues.contains(dataValue.toString());
                }
            } else {
                //普通筛选器
                SQLBinaryOperator predicateSymbol = entity.getPredicateSymbol();
                if (SQLBinaryOperator.Equality.equals(predicateSymbol)) {
                    // =
                    predicte = sqlValue.toString().equals(dataValue.toString());
                } else if (SQLBinaryOperator.GreaterThanOrEqual.equals(predicateSymbol)) {
                    // >=
                    int res = StringUtil.compare(dataValue.toString(), sqlValue.toString());
                    predicte = res >= 0;
                } else if (SQLBinaryOperator.LessThanOrEqual.equals(predicateSymbol)) {
                    // <=
                    int res = StringUtil.compare(dataValue.toString(), sqlValue.toString());
                    predicte = res <= 0;
                } else if (SQLBinaryOperator.NotEqual.equals(predicateSymbol)) {
                    // !=
                    predicte = !sqlValue.equals(dataValue.toString());
                } else if (SQLBinaryOperator.GreaterThan.equals(predicateSymbol)) {
                    // >
                    int res = StringUtil.compare(dataValue.toString(), sqlValue.toString());
                    predicte = res > 0;
                } else if (SQLBinaryOperator.LessThan.equals(predicateSymbol)) {
                    // <
                    int res = StringUtil.compare(dataValue.toString(), sqlValue.toString());
                    predicte = res < 0;
                } else if (SQLBinaryOperator.IsNot.equals(predicateSymbol)) {
                    // is not null
                    predicte = !(dataValue == null || dataValue.toString().length() == 0);
                } else if (SQLBinaryOperator.Is.equals(predicateSymbol)) {
                    // is null
                    predicte = (dataValue == null || dataValue.toString().length() == 0);

                } else if (SQLBinaryOperator.Like.equals(predicateSymbol)) {
                    // like
                    String s = sqlValue.toString();

                    if (s.startsWith("%") && s.endsWith("%")) {
                        //包含
                        String substring = s.substring(1, s.length() - 1);
                        predicte = dataValue.toString().contains(substring);
                    } else if (s.startsWith("%")) {
                        //以...结尾
                        String substring = s.substring(1);
                        predicte = dataValue.toString().endsWith(substring);
                    } else if (s.endsWith("%")) {
                        // 以...开头
                        String substring = s.substring(0, s.length() - 1);
                        predicte = dataValue.toString().startsWith(substring);
                    } else {
                        //相当于 =
                        predicte = sqlValue.toString().equals(dataValue.toString());
                    }

                }
            }
            str.append(predicte);
        }

        Object eval = null;
        try {
            eval = scriptEngine.eval(str.toString());
        } catch (ScriptException e) {
            log.error("can not execute this script {}", str.toString(), e);
            e.printStackTrace();
        }
        return (Boolean) eval;
    }


    public static String toInsertSql(DataFrame df,String tableName){
        if(df == null || df.length() ==0){
            throw new NullPointerException("dataframe is can not be empty");
        }
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(tableName).append(" (");
        Set columns = df.columns();
        columns.forEach(field -> {
            sb.append(field).append(",");
        });
        sb.deleteCharAt(sb.lastIndexOf(","));
        sb.append(") ").append("VALUES");
        for (Object o : df) {
            List column = (List)o;
            sb.append(" (");
            for (Object value : column) {
                sb.append("'").append(value).append("'").append(",");
            }
            sb.deleteCharAt(sb.lastIndexOf(","));
            sb.append(")").append(",");
        }
        sb.deleteCharAt(sb.lastIndexOf(","));
        return sb.toString();
    }
}
