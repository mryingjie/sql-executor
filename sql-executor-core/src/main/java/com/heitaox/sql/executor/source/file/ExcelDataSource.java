package com.heitaox.sql.executor.source.file;

import com.heitaox.sql.executor.core.entity.ExcelData;
import com.heitaox.sql.executor.core.entity.PredicateEntity;
import com.heitaox.sql.executor.core.util.ClassConvertUtil;
import com.heitaox.sql.executor.source.FileDataSource;
import joinery.DataFrame;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;


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

        List<ExcelData> excelDataList  = null;
        DataFrame<Object> dataFrame = new DataFrame<>();
        try {
            excelDataList = ExcelUtil.readExcel(filePath);
            if (CollectionUtils.isEmpty(excelDataList)) {
                return dataFrame;
            }
            ExcelData excelData = excelDataList.get(0);
            List<List<String>> rows = excelData.getRows();
            Map<Integer, Class> indexToType = new HashMap<>();
            for (int i = 0; i < rows.size(); i++) {
                List<String> row = rows.get(i);
                if (i == 0) {
                    Object[] columnsWithAlias = row.stream().map(column -> tableAlias + "." + column).toArray();
                    dataFrame.add(columnsWithAlias);
                    for (int j = 0; j < row.size(); j++) {
                        Class aClass = schema.get(row.get(i));
                        if (aClass != null) {
                            indexToType.put(j, aClass);
                        }
                    }
                } else {
                    List<Object> objects = new ArrayList<>();
                    for (int j = 0; j < row.size(); j++) {
                        Class aClass = indexToType.get(j);
                        String column = row.get(j);
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
            List<String> head = ExcelUtil.readExcelHead(filePath);
            List<List<Object>> lines = new ArrayList<>();
            for (Map<String, Object> lineMap : valueList) {
                List<Object> line = new ArrayList<>();
                for (String s : head) {
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
