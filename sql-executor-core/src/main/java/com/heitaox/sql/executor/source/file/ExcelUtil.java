package com.heitaox.sql.executor.source.file;

import com.heitaox.sql.executor.core.entity.PredicateEntity;
import com.heitaox.sql.executor.core.util.DataFrameUntil;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class ExcelUtil {

    public static String readExcelHead(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new RuntimeException("file [" + filePath + "] not fond");
        }
        String suffix = getSuffix(filePath);

        // 返回值列
        String headStr = "";
        if (".xls".equals(suffix)) {
            headStr = readExcel2003Head(filePath);
        } else if (".xlsx".equals(suffix)) {
            headStr = readExcel2007Head(filePath);
        } else {
            headStr = readCsvHead(filePath);
        }

        return headStr;
    }


    private static String readExcel2007Head(String filePath) throws IOException {
        // 返回结果集
        FileInputStream fis = null;


        StringBuilder sb = new StringBuilder();
        try {
            fis = new FileInputStream(filePath);
            XSSFWorkbook wookbook = new XSSFWorkbook(fis); // 创建对Excel工作簿文件的引用
            // 遍历所有sheet
            Map<String, String> sheet_map = new HashMap<>();

            XSSFSheet sheet = wookbook.getSheetAt(0); // 在Excel文档中，第page张工作表的缺省索引是0
            int cells = 0;// 当前sheet的行数
            // 遍历sheet中所有的行
            XSSFRow firstRow = sheet.getRow(0);
            if (firstRow != null) {
                // 获取到Excel文件中的所有的列
                cells = firstRow.getPhysicalNumberOfCells();
                // 遍历列
                for (int j = 0; j < cells; j++) {
                    // 获取到列的值
                    XSSFCell cell = firstRow.getCell(j);
                    String cellValue = getCellValue(cell);
                    if (cellValue == null) {
                        cellValue = "";
                    }
                    sb.append(cellValue).append(ExcelContant.SEPARATOR);
                }
            }
            //封装每个sheet入map
        } finally {
            if (fis != null) {

                fis.close();
            }
        }
        return sb.toString();
    }

    private static String readExcel2003Head(String filePath) throws IOException {
        // 返回结果集
        FileInputStream fis = null;

        StringBuilder sb = null;

        try {
            fis = new FileInputStream(filePath);
            HSSFWorkbook wookbook = new HSSFWorkbook(fis); // 创建对Excel工作簿文件的引用
            // 遍历所有sheet

            HSSFSheet sheet = wookbook.getSheetAt(0); // 在Excel文档中，第page张工作表的缺省索引是0
            // 遍历sheet中所有的行
            HSSFRow firstRow = sheet.getRow(0);
            int cells = firstRow.getPhysicalNumberOfCells();
            sb = new StringBuilder();
            for (int j = 0; j < cells; j++) {
                // 获取到列的值
                HSSFCell cell = firstRow.getCell(j);
                String cellValue = getCellValue(cell);
                if (cellValue == null) {
                    cellValue = "";
                }
                sb.append(cellValue).append(ExcelContant.SEPARATOR);
            }
        } finally {
            if (fis != null) {

                fis.close();
            }
        }
        return sb.toString();
    }

    /**
     * 读取excel
     *
     * @param filePath filePath
     * @return List
     * @throws IOException IOException
     */
    public static List<Map<String, String>> readExcel(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new RuntimeException("file [" + filePath + "] not fond");
        }
        String suffix = getSuffix(filePath);

        // 返回值列
        List<Map<String, String>> reaultList = new ArrayList<Map<String, String>>();
        if (".xls".equals(suffix)) {
            reaultList = readExcel2003(filePath);
        } else if (".xlsx".equals(suffix)) {
            reaultList = readExcel2007(filePath);
        } else {
            reaultList = readCsv(filePath);
        }

        return reaultList;
    }

    public static String getSuffix(String filePath) {
        String suffix;
        if (filePath.contains(".xlsx")) {
            suffix = ".xlsx";
        } else if (filePath.contains(".xls")) {
            suffix = ".xls";
        } else if (filePath.contains(".csv")) {
            suffix = ".csv";
        } else {
            throw new RuntimeException("file [" + filePath + "] is not supported. Please use a file in .xls or .xlsx or .csv format ");
        }
        return suffix;
    }


    /**
     * 读取97-2003格式(即xls格式)
     */
    private static List<Map<String, String>> readExcel2003(String filePath) throws IOException {
        // 返回结果集
        List<Map<String, String>> resurtListMap = new ArrayList<Map<String, String>>();

        File file = new File(filePath);
        String fileName = file.getName();
        try (FileInputStream fis = new FileInputStream(filePath)) {
            HSSFWorkbook wookbook = new HSSFWorkbook(fis); // 创建对Excel工作簿文件的引用
            // 遍历所有sheet
            for (int page = 0; page < wookbook.getNumberOfSheets(); page++) {
                Map<String, String> sheetMap = new HashMap<>();

                HSSFSheet sheet = wookbook.getSheetAt(page); // 在Excel文档中，第page张工作表的缺省索引是0
                int rows = sheet.getPhysicalNumberOfRows(); // 获取到Excel文件中的所有行数
                String sheetName = sheet.getSheetName();// sheet名称，用于校验模板是否正确
                StringBuilder sb = new StringBuilder();
                int cells = 0;// 当前sheet的行数
                // 遍历sheet中所有的行
                HSSFRow firstRow = sheet.getRow(page);
                if (firstRow != null) {
                    // 获取到Excel文件中的所有的列
                    cells = firstRow.getPhysicalNumberOfCells();
                    for (int i = 0; i < rows; i++) {
                        // 读取左上端单元格
                        HSSFRow row = sheet.getRow(i);
                        // 行不为空
                        if (row != null) {
                            boolean isValidRow = false;
                            // 遍历列
                            for (int j = 0; j < cells; j++) {
                                // 获取到列的值
                                HSSFCell cell = row.getCell(j);
                                String cellValue = getCellValue(cell);
                                if (cellValue == null) {
                                    cellValue = "";
                                }
                                sb.append(cellValue).append(ExcelContant.SEPARATOR);
                                if (!isValidRow && cellValue.trim().length() > 0) {
                                    isValidRow = true;
                                }
                            }
                            sb.append(ExcelContant.WRAP);
                        }
                    }
                }
                //封装每个sheet入map
                sheetMap.put(ExcelContant.FILE_NAME, fileName);
                sheetMap.put(ExcelContant.SHEET_NAME, sheetName);
                sheetMap.put(ExcelContant.SHEET_CONTENT, sb.toString());
                resurtListMap.add(sheetMap);
            }
        }
        return resurtListMap;
    }

    public static int updateValue2003(Map<String, Object> updateItems, List<PredicateEntity<Object>> predicateEntities, String filePath, String tableName) throws IOException {
        File file = new File(filePath);
        FileOutputStream out = null;
        int update = 0;
        try {
            HSSFWorkbook workbook = new HSSFWorkbook(new FileInputStream(file));
            HSSFSheet sheet = workbook.getSheet(tableName);
            int rows = sheet.getPhysicalNumberOfRows();//所有行数
            // 遍历sheet中所有的行
            HSSFRow firstRow = sheet.getRow(0);//表头
            Map<String, Integer> columnToIndex = new HashMap<>();
            int index = 0;
            for (Cell cell : firstRow) {
                String cellValue = getCellValue((HSSFCell) cell);
                columnToIndex.put(cellValue, index++);
            }
            int cells = firstRow.getPhysicalNumberOfCells();
            // 获取到Excel文件中的所有的列
            List<Object> values = new ArrayList<>(cells);
            for (int i = 1; i < rows; i++) {
                // 读取左上端单元格
                HSSFRow row = sheet.getRow(i);
                // 行不为空
                if (row != null) {
                    // 遍历列
                    for (int j = 0; j < cells; j++) {
                        // 获取到列的值
                        HSSFCell cell = row.getCell(j);
                        String cellValue = getCellValue(cell);
                        values.add(cellValue);
                    }
                    Boolean match = DataFrameUntil.exectuePredicaeEntity(predicateEntities, columnToIndex, values, tableName);
                    if (match) {
                        for (Map.Entry<String, Object> entry : updateItems.entrySet()) {
                            Object value = entry.getValue();
                            String key = entry.getKey();
                            row.getCell(columnToIndex.get(key)).setCellValue(value.toString());
                        }
                        update++;
                    }
                    values.clear();
                }

            }
            out = new FileOutputStream(filePath);
            workbook.write(out);
        } finally {
            if (out != null) {
                out.close();
            }
        }

        return update;
    }


    public static int deleteValue2007(List<PredicateEntity<Object>> predicateEntities, String filePath, String tableName) throws IOException {
        File file = new File(filePath);
        FileOutputStream out = null;
        int delete = 0;
        try {
            XSSFWorkbook workbook = new XSSFWorkbook(new FileInputStream(file));
            XSSFSheet sheet = workbook.getSheet(tableName);
            int rows = sheet.getPhysicalNumberOfRows();//所有行数
            // 遍历sheet中所有的行
            XSSFRow firstRow = sheet.getRow(0);//表头
            Map<String, Integer> columnToIndex = new HashMap<>();
            int index = 0;
            for (Cell cell : firstRow) {
                String cellValue = getCellValue((XSSFCell) cell);
                columnToIndex.put(cellValue, index++);
            }
            int cells = firstRow.getPhysicalNumberOfCells();
            // 获取到Excel文件中的所有的列
            List<Object> values = new ArrayList<>(cells);
            List<Integer> deleteRowNum = new ArrayList<>();
            for (int i = 1; i < rows; i++) {
                // 读取左上端单元格
                XSSFRow row = sheet.getRow(i);
                // 行不为空
                if (row != null) {
                    // 遍历列
                    for (int j = 0; j < cells; j++) {
                        // 获取到列的值
                        XSSFCell cell = row.getCell(j);
                        String cellValue = getCellValue(cell);
                        values.add(cellValue);
                    }
                    Boolean match = DataFrameUntil.exectuePredicaeEntity(predicateEntities, columnToIndex, values, tableName);
                    if (match) {
                        deleteRowNum.add(row.getRowNum());
                        delete++;
                    }
                    values.clear();
                }

            }
            int deleted = 0;
            int lastRowNum = sheet.getLastRowNum();
            for (Integer rowNum : deleteRowNum) {
                sheet.shiftRows(rowNum + 1 - deleted, lastRowNum, -1);
                deleted++;
            }
            out = new FileOutputStream(filePath);
            workbook.write(out);
        } finally {
            if (out != null) {
                out.close();
            }
        }

        return delete;
    }
    public static int deleteValue2003(List<PredicateEntity<Object>> predicateEntities, String filePath, String tableName) throws IOException {
        File file = new File(filePath);
        FileOutputStream out = null;
        int delete = 0;
        try {
            HSSFWorkbook workbook = new HSSFWorkbook(new FileInputStream(file));
            HSSFSheet sheet = workbook.getSheet(tableName);
            int rows = sheet.getPhysicalNumberOfRows();//所有行数
            // 遍历sheet中所有的行
            HSSFRow firstRow = sheet.getRow(0);//表头
            Map<String, Integer> columnToIndex = new HashMap<>();
            int index = 0;
            for (Cell cell : firstRow) {
                String cellValue = getCellValue((HSSFCell) cell);
                columnToIndex.put(cellValue, index++);
            }
            int cells = firstRow.getPhysicalNumberOfCells();
            // 获取到Excel文件中的所有的列
            List<Object> values = new ArrayList<>(cells);
            List<Integer> deleteRowNum = new ArrayList<>();
            for (int i = 1; i < rows; i++) {
                // 读取左上端单元格
                HSSFRow row = sheet.getRow(i);
                // 行不为空
                if (row != null) {
                    // 遍历列
                    for (int j = 0; j < cells; j++) {
                        // 获取到列的值
                        HSSFCell cell = row.getCell(j);
                        String cellValue = getCellValue(cell);
                        values.add(cellValue);
                    }
                    Boolean match = DataFrameUntil.exectuePredicaeEntity(predicateEntities, columnToIndex, values, tableName);
                    if (match) {
                        deleteRowNum.add(row.getRowNum());
                        delete++;
                    }
                    values.clear();
                }

            }
            int deleted = 0;
            int lastRowNum = sheet.getLastRowNum();
            for (Integer rowNum : deleteRowNum) {
                sheet.shiftRows(rowNum + 1 - deleted, lastRowNum, -1);
                deleted++;
            }
            for (int i = 0; i < delete; i++) {
                sheet.removeRow(sheet.getRow(lastRowNum-i));
            }
            out = new FileOutputStream(filePath);
            workbook.write(out);
        } finally {
            if (out != null) {
                out.close();
            }
        }

        return delete;
    }

    public static int updateValue2007(Map<String, Object> updateItems, List<PredicateEntity<Object>> predicateEntities, String filePath, String tableName) throws IOException {
        File file = new File(filePath);
        FileOutputStream out = null;
        int update = 0;
        try {
            XSSFWorkbook workbook = new XSSFWorkbook(new FileInputStream(file));
            XSSFSheet sheet = workbook.getSheet(tableName);
            int rows = sheet.getPhysicalNumberOfRows();//所有行数
            // 遍历sheet中所有的行
            XSSFRow firstRow = sheet.getRow(0);//表头
            Map<String, Integer> columnToIndex = new HashMap<>();
            int index = 0;
            for (Cell cell : firstRow) {
                String cellValue = getCellValue((XSSFCell) cell);
                columnToIndex.put(cellValue, index++);
            }
            int cells = firstRow.getPhysicalNumberOfCells();
            // 获取到Excel文件中的所有的列
            List<Object> values = new ArrayList<>(cells);
            for (int i = 1; i < rows; i++) {
                // 读取左上端单元格
                XSSFRow row = sheet.getRow(i);
                // 行不为空
                if (row != null) {
                    // 遍历列
                    for (int j = 0; j < cells; j++) {
                        // 获取到列的值
                        XSSFCell cell = row.getCell(j);
                        String cellValue = getCellValue(cell);
                        values.add(cellValue);
                    }
                    Boolean match = DataFrameUntil.exectuePredicaeEntity(predicateEntities, columnToIndex, values, tableName);
                    if (match) {
                        for (Map.Entry<String, Object> entry : updateItems.entrySet()) {
                            Object value = entry.getValue();
                            String key = entry.getKey();
                            row.getCell(columnToIndex.get(key)).setCellValue(value.toString());
                        }
                        update++;
                    }
                    values.clear();
                }

            }
            out = new FileOutputStream(filePath);
            workbook.write(out);
        } finally {
            if (out != null) {
                out.close();
            }
        }

        return update;
    }

    /**
     * 读取2007格式(即xlsx)
     */
    private static List<Map<String, String>> readExcel2007(String filePath) throws IOException {
        // 返回结果集
        List<Map<String, String>> resurtListMap = new ArrayList<Map<String, String>>();

        File file = new File(filePath);
        String fileName = file.getName();

        try (FileInputStream fis = new FileInputStream(filePath)) {
            XSSFWorkbook wookbook = new XSSFWorkbook(fis); // 创建对Excel工作簿文件的引用
            // 遍历所有sheet
            for (int page = 0; page < wookbook.getNumberOfSheets(); page++) {
                Map<String, String> sheet_map = new HashMap<>();

                XSSFSheet sheet = wookbook.getSheetAt(page); // 在Excel文档中，第page张工作表的缺省索引是0
                int rows = sheet.getPhysicalNumberOfRows(); // 获取到Excel文件中的所有行数
                String sheetName = sheet.getSheetName();// sheet名称，用于校验模板是否正确
                StringBuilder sb = new StringBuilder();
                int cells = 0;// 当前sheet的行数
                // 遍历sheet中所有的行
                XSSFRow firstRow = sheet.getRow(page);
                if (firstRow != null) {
                    // 获取到Excel文件中的所有的列
                    cells = firstRow.getPhysicalNumberOfCells();
                    for (int i = 0; i < rows; i++) {
                        // 读取左上端单元格
                        XSSFRow row = sheet.getRow(i);
                        // 行不为空
                        if (row != null) {
                            boolean isValidRow = false;
                            // 遍历列
                            for (int j = 0; j < cells; j++) {
                                // 获取到列的值
                                XSSFCell cell = row.getCell(j);
                                String cellValue = getCellValue(cell);
                                if (cellValue == null) {
                                    cellValue = "";
                                }
                                sb.append(cellValue).append(ExcelContant.SEPARATOR);
                                if (!isValidRow && cellValue.trim().length() > 0) {
                                    isValidRow = true;
                                }
                            }
                            sb.append(ExcelContant.WRAP);
                        }
                    }
                }
                //封装每个sheet入map
                sheet_map.put(ExcelContant.FILE_NAME, fileName);
                sheet_map.put(ExcelContant.SHEET_NAME, sheetName);
                sheet_map.put(ExcelContant.SHEET_CONTENT, sb.toString());
                resurtListMap.add(sheet_map);
            }

        }
        return resurtListMap;
    }


    private static String getCellValue(HSSFCell cell) {
        DecimalFormat df = new DecimalFormat("#");
        String cellValue = null;
        if (cell == null)
            return null;
        switch (cell.getCellType()) {
            case HSSFCell.CELL_TYPE_NUMERIC:
                if (HSSFDateUtil.isCellDateFormatted(cell)) {
                    SimpleDateFormat sdf = new SimpleDateFormat(
                            "yyyy-MM-dd HH:mm:ss");
                    cellValue = sdf.format(HSSFDateUtil.getJavaDate(cell
                            .getNumericCellValue()));
                    break;
                }
                cellValue = df.format(cell.getNumericCellValue());
                break;
            case HSSFCell.CELL_TYPE_STRING:
                cellValue = String.valueOf(cell.getStringCellValue());
                break;
            case HSSFCell.CELL_TYPE_FORMULA:
                cellValue = String.valueOf(cell.getCellFormula());
                break;
            case HSSFCell.CELL_TYPE_BLANK:
                cellValue = null;
                break;
            case HSSFCell.CELL_TYPE_BOOLEAN:
                cellValue = String.valueOf(cell.getBooleanCellValue());
                break;
            case HSSFCell.CELL_TYPE_ERROR:
                cellValue = String.valueOf(cell.getErrorCellValue());
                break;
        }
        if (cellValue != null && cellValue.trim().length() <= 0) {
            cellValue = null;
        }
        return cellValue;
    }

    private static String getCellValue(XSSFCell cell) {
        DecimalFormat df = new DecimalFormat("#");
        String cellValue = null;
        if (cell == null)
            return null;
        switch (cell.getCellType()) {
            case XSSFCell.CELL_TYPE_NUMERIC:
                if (HSSFDateUtil.isCellDateFormatted(cell)) {
                    SimpleDateFormat sdf = new SimpleDateFormat(
                            "yyyy-MM-dd HH:mm:ss");
                    cellValue = sdf.format(HSSFDateUtil.getJavaDate(cell
                            .getNumericCellValue()));
                    break;
                }
                cellValue = df.format(cell.getNumericCellValue());
                break;
            case XSSFCell.CELL_TYPE_STRING:
                cellValue = String.valueOf(cell.getStringCellValue());
                break;
            case XSSFCell.CELL_TYPE_FORMULA:
                cellValue = String.valueOf(cell.getCellFormula());
                break;
            case XSSFCell.CELL_TYPE_BLANK:
                cellValue = null;
                break;
            case XSSFCell.CELL_TYPE_BOOLEAN:
                cellValue = String.valueOf(cell.getBooleanCellValue());
                break;
            case XSSFCell.CELL_TYPE_ERROR:
                cellValue = String.valueOf(cell.getErrorCellValue());
                break;
        }
        if (cellValue != null && cellValue.trim().length() <= 0) {
            cellValue = null;
        }
        return cellValue;
    }

    public static int appendValue2007(List<Map<String, Object>> valueList, String filePath, String tableName) throws IOException {
        File file = new File(filePath);
        FileOutputStream out = null;
        int i = 0;
        try {
            XSSFWorkbook workbook = new XSSFWorkbook(new FileInputStream(file));
            XSSFSheet sheet = workbook.getSheet(tableName);

            int rowId = sheet.getLastRowNum() + 1; // 获取表格的总行数
            int columnCount = sheet.getRow(0).getLastCellNum();// 获取表头的列数

            XSSFRow titleRow = sheet.getRow(0);
            if (titleRow != null) {
                for (Map<String, Object> map : valueList) {
                    XSSFRow newRow = sheet.createRow(rowId++);
                    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                        String mapKey = titleRow.getCell(columnIndex).toString().trim();
                        XSSFCell cell = newRow.createCell(columnIndex);
                        cell.setCellValue(map.get(mapKey) == null ? "" : map.get(mapKey).toString());
                    }
                    i++;
                }
            }
            out = new FileOutputStream(filePath);
            workbook.write(out);
        } finally {
            if (out != null) {
                out.close();
            }

        }

        return i;
    }

    public static int appendValue2003(List<Map<String, Object>> valueList, String filePath, String tableName) throws IOException {
        File file = new File(filePath);
        FileOutputStream out = null;
        int i = 0;
        try {
            HSSFWorkbook workbook = new HSSFWorkbook(new FileInputStream(file));
            HSSFSheet sheet = workbook.getSheet(tableName);
            int rowId = sheet.getLastRowNum() + 1; // 获取表格的总行数
            int columnCount = sheet.getRow(0).getLastCellNum();// 获取表头的列数
            HSSFRow titleRow = sheet.getRow(0);
            if (titleRow != null) {
                for (Map<String, Object> map : valueList) {
                    HSSFRow newRow = sheet.createRow(rowId++);
                    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                        String mapKey = titleRow.getCell(columnIndex).toString().trim();
                        HSSFCell cell = newRow.createCell(columnIndex);
                        cell.setCellValue(map.get(mapKey) == null ? "" : map.get(mapKey).toString());
                    }
                    i++;
                }
            }
            out = new FileOutputStream(filePath);
            workbook.write(out);
        } finally {
            if (out != null) {
                out.close();
            }
        }

        return i;
    }

    public static int appendValueCsv(List<List<Object>> lines, String filePath) throws IOException {
        RandomAccessFile randomAccessFile = null;
        int i = 0;
        try {
            randomAccessFile = new RandomAccessFile(filePath, "rw");

            for (List<Object> line : lines) {
                long fileLength = randomAccessFile.length();
                randomAccessFile.seek(fileLength);
                for (Object o : line) {
                    randomAccessFile.write((o.toString() + ExcelContant.SEPARATOR).getBytes("GBK"));
                }
                randomAccessFile.write(ExcelContant.WRAP.getBytes("GBK"));
                i++;
            }
        } finally {
            if (randomAccessFile != null) {
                randomAccessFile.close();
            }

        }
        return i;
    }

    public static int updateValueCsv(Map<String, Object> updateItems, List<PredicateEntity<Object>> predicateEntities, String filePath, String tableName) throws IOException {
        // 返回结果集
        int update = 0;
        File file = new File(filePath);
        StringBuilder sb = new StringBuilder();
        FileInputStream fis = new FileInputStream(file);
        InputStreamReader isr = new InputStreamReader(fis, "GBK");

        try (BufferedReader br = new BufferedReader(isr)) {
            String line;
            line = br.readLine();
            if (line == null) {
                return 0;
            }
            Map<String, Integer> columnToindex = new HashMap<>();
            String[] split = line.split(ExcelContant.SEPARATOR);
            for (int i = 0; i < split.length; i++) {
                columnToindex.put(split[i], i);
            }
            sb.append(line).append(ExcelContant.WRAP);

            while ((line = br.readLine()) != null) {
                split = line.split(ExcelContant.SEPARATOR);
                Boolean match = DataFrameUntil.exectuePredicaeEntity(predicateEntities, columnToindex, Arrays.asList(split), tableName);
                if (match) {
                    for (Map.Entry<String, Object> entry : updateItems.entrySet()) {
                        Object value = entry.getValue();
                        String key = entry.getKey();
                        Integer integer = columnToindex.get(key);
                        split[integer] = value.toString();
                    }
                    for (String s : split) {
                        sb.append(s).append(ExcelContant.SEPARATOR);
                    }
                    sb.append(ExcelContant.WRAP);
                    update++;
                } else {
                    sb.append(line).append(ExcelContant.WRAP);
                }

            }
        }
        file.delete();
        file.createNewFile();
        FileOutputStream fos = new FileOutputStream(file);
        OutputStreamWriter os = new OutputStreamWriter(fos, "GBK");
        try (BufferedWriter bw = new BufferedWriter(os)) {
            bw.write(sb.toString());
        }
        return update;
    }

    private static String readCsvHead(String filePath) {
        // 返回结果集
        File file = new File(filePath);
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        String line = null;
        try {
            fis = new FileInputStream(file);
            isr = new InputStreamReader(fis, "GBK");
            br = new BufferedReader(isr);
            line = br.readLine();
        } catch (IOException ex) {
            log.error("文件[{}]读取失败", filePath, ex);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    log.error("流关闭失败");
                }
            }
        }
        return line;
    }

    private static List<Map<String, String>> readCsv(String filePath) throws IOException {
        // 返回结果集
        List<Map<String, String>> resurtListMap = new ArrayList<Map<String, String>>();

        File file = new File(filePath);
        String fileName = file.getName();
        FileInputStream fis = null;
        InputStreamReader isr = null;
        StringBuilder sb = new StringBuilder();
        fis = new FileInputStream(file);
        isr = new InputStreamReader(fis, "GBK");
        try (BufferedReader br = new BufferedReader(isr)) {
            Map<String, String> sheetMap = new HashMap<>();
            String line;

            while ((line = br.readLine()) != null) {
                sb.append(line).append(ExcelContant.WRAP);
            }
            sheetMap.put(ExcelContant.SHEET_NAME, fileName);
            sheetMap.put(ExcelContant.FILE_NAME, fileName);
            sheetMap.put(ExcelContant.SHEET_CONTENT, sb.toString());
            resurtListMap.add(sheetMap);
        }

        return resurtListMap;
    }

    public static int deleteValueCsv(List<PredicateEntity<Object>> predicateEntities, String filePath, String tableName) throws IOException {
        // 返回结果集
        int delete = 0;
        File file = new File(filePath);
        StringBuilder sb = new StringBuilder();
        FileInputStream fis = new FileInputStream(file);
        InputStreamReader isr = new InputStreamReader(fis, "GBK");

        try (BufferedReader br = new BufferedReader(isr)) {
            String line;
            line = br.readLine();
            if (line == null) {
                return 0;
            }
            Map<String, Integer> columnToindex = new HashMap<>();
            String[] split = line.split(ExcelContant.SEPARATOR);
            for (int i = 0; i < split.length; i++) {
                columnToindex.put(split[i], i);
            }
            sb.append(line).append(ExcelContant.WRAP);
            while ((line = br.readLine()) != null) {
                split = line.split(ExcelContant.SEPARATOR);
                Boolean match = DataFrameUntil.exectuePredicaeEntity(predicateEntities, columnToindex, Arrays.asList(split), tableName);
                if (!match) {
                    sb.append(line).append(ExcelContant.WRAP);
                }
            }
        }
        file.delete();
        file.createNewFile();
        FileOutputStream fos = new FileOutputStream(file);
        OutputStreamWriter os = new OutputStreamWriter(fos, "GBK");
        try (BufferedWriter bw = new BufferedWriter(os)) {
            bw.write(sb.toString());
            bw.flush();
        }
        return delete;
    }


}
