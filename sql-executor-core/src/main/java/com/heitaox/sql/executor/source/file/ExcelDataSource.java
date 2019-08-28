package com.heitaox.sql.executor.source.file;

import com.heitaox.sql.executor.core.entity.PredicateEntity;
import com.heitaox.sql.executor.core.util.ClassConvertUtil;
import com.heitaox.sql.executor.source.FileDataSource;
import joinery.DataFrame;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @Author ZhengYingjie
 * @Date 2019-08-06
 * @Description
 */
@AllArgsConstructor
@Slf4j
public class ExcelDataSource implements FileDataSource {

    private final String filePath;

    /**
     * 字段类型  只需传不是String类型的基本类型或者对应的包装类型字段
     */
    private final Map<String, Class> schema;

    @Override
    public DataFrame<Object> queryAll(String tableNme, String tableAlias) {

        List<Map<String, String>> maps = null;
        DataFrame<Object> dataFrame = new DataFrame<>();
        try {
            maps = ExcelUtil.readExcel(filePath);
            if (maps == null || maps.size() == 0) {
                return dataFrame;
            }
            Map<String, String> map = maps.get(0);
            String data = map.get(ExcelContant.SHEET_CONTENT);
            String[] split = data.split(ExcelContant.WRAP);
            Map<Integer, Class> indexToType = new HashMap<>();
            for (int i = 0; i < split.length; i++) {
                String[] columns = split[i].split(ExcelContant.SEPARATOR);
                if (i == 0) {
                    Object[] columnsWithAlias = Stream.of(columns).map(column -> tableAlias + "." + column).toArray();
                    dataFrame.add(columnsWithAlias);
                    for (int j = 0; j < columns.length; j++) {
                        Class aClass = schema.get(columns[j]);
                        if (aClass != null) {
                            indexToType.put(j, aClass);
                        }
                    }
                } else {
                    ArrayList<Object> objects = new ArrayList<>();
                    for (int j = 0; j < columns.length; j++) {
                        Class aClass = indexToType.get(j);
                        String column = columns[j];
                        if (aClass == null) {
                            objects.add(column);
                        } else {
                            objects.add(ClassConvertUtil.convertClass(aClass, column));
                        }
                    }
                    dataFrame.append(objects);
                }
            }


        } catch (Exception e) {
            log.error("读取文件[{}]数据失败", tableNme, e);
        }
        return dataFrame;
    }

    @Override
    public int insert(List<Map<String, Object>> valueList, String tableName) throws IOException {
        String suffix = ExcelUtil.getSuffix(filePath);
        int i = 0;
        if (".xls".equals(suffix)) {
            i = ExcelUtil.appendValue2003(valueList,filePath,tableName);
        } else if (".xlsx".equals(suffix)) {
            i = ExcelUtil.appendValue2007(valueList,filePath,tableName);
        } else {
            String headStr = ExcelUtil.readExcelHead(filePath);
            String[] split = headStr.split(",");
            List<List<Object>> lines = new ArrayList<>();
            for (Map<String, Object> lineMap : valueList) {
                List<Object> line = new ArrayList<>();
                for (Object s : split) {
                    line.add(lineMap.get(s));
                }
                lines.add(line);
            }
            i = ExcelUtil.appendValueCsv(lines, filePath);
        }


        return i;
    }

    @Override
    public int update(Map<String, Object> updateItems, List<PredicateEntity<Object>> predicateEntities, String tableName) throws IOException {
        String suffix = ExcelUtil.getSuffix(filePath);
        int i = 0;
        if (".xls".equals(suffix)) {
            i = ExcelUtil.updateValue2003(updateItems,predicateEntities,filePath,tableName);
        } else if (".xlsx".equals(suffix)) {
            i = ExcelUtil.updateValue2007(updateItems,predicateEntities,filePath,tableName);
        } else {

            i = ExcelUtil.updateValueCsv(updateItems,predicateEntities,filePath,tableName);
        }
        return i;
    }

    @Override
    public int delete(List<PredicateEntity<Object>> predicateEntities, String tableName) throws IOException {
        String suffix = ExcelUtil.getSuffix(filePath);
        int i = 0;
        if (".xls".equals(suffix)) {
            i = ExcelUtil.deleteValue2003(predicateEntities,filePath,tableName);
        } else if (".xlsx".equals(suffix)) {
            i = ExcelUtil.deleteValue2007(predicateEntities,filePath,tableName);
        } else {

            i = ExcelUtil.deleteValueCsv(predicateEntities,filePath,tableName);
        }
        return i;
    }


}
