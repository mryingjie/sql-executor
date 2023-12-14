package com.heitaox.sql.executor.core.entity;

import lombok.Data;

import java.util.List;

/**
 * @author Yingjie Zheng
 * @date 2023/12/14 14:23
 * @description
 */
@Data
public class ExcelData {

    private String fileName;

    private String sheetName;

    private List<List<String>> rows;

}
