package com.heitaox.sql.executor;

import com.heitaox.sql.executor.source.file.ExcelDataSource;
import joinery.DataFrame;
import lombok.SneakyThrows;

import java.util.*;

/**
 * @author Yingjie Zheng
 * @date 2023/5/15 19:47
 * @description
 */
public class Main {


    @SneakyThrows
    public static void main(String[] args) {


        SQLExecutor.SQLExecutorBuilder builder = new SQLExecutor.SQLExecutorBuilder();
        SQLExecutor sqlExecutor = builder
                .putDataSource("t_whiteList", new ExcelDataSource("/Users/kiko/Documents/合同产研开白名单1.xlsx", new HashMap<>()))
                .enableCache()
                .enableFilterBeforeJoin()
                .build();

        DataFrame dataFrame = sqlExecutor.executeQuery("select 供应商ID,供应商类型,开白渠道 from t_whiteList");



        for (int i = 0; i < dataFrame.length(); i++) {
            String supplierId = (String) dataFrame.get(i, "供应商ID");
            String identitiesStr = (String) dataFrame.get(i, "供应商类型");
            String businessTypeStr = (String) dataFrame.get(i, "开白渠道");
        }
        System.out.println(dataFrame);

    }

}
