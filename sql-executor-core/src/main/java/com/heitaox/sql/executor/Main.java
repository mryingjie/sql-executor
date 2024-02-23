package com.heitaox.sql.executor;

import com.heitaox.sql.executor.source.file.ExcelDataSource;
import joinery.DataFrame;
import lombok.SneakyThrows;

import java.time.LocalDate;
import java.time.LocalDateTime;
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
                .putDataSource("tb_supplier", new ExcelDataSource("/Users/kiko/IdeaProjects/self/sql-executor/sql-executor-core/src/main/resources/db_supplier_tb_supplier.xlsx", new HashMap<>()))
                .putDataSource("tb_supplier_shop", new ExcelDataSource("/Users/kiko/IdeaProjects/self/sql-executor/sql-executor-core/src/main/resources/db_supplier_tb_supplier_shop.xlsx", new HashMap<>()))
                .enableCache()
                .enableFilterBeforeJoin()
                .build();

        DataFrame dataFrame = sqlExecutor.executeQuery("select * from tb_supplier as supplier" +
                " left join tb_supplier_shop as shop on supplier.supplier_no = shop.supplier_no");

        Set columns = dataFrame.columns();
        for (Object column : columns) {
            System.out.print(column);
            System.out.print("\t\t");
        }
        int size = columns.size();
        System.out.print("\n");
        for (int i = 0; i < dataFrame.length(); i++) {
            List row = dataFrame.row(i);
            for (int j = 0; j < size; j++) {
                System.out.print(row.get(j));
                System.out.print("\t\t");
            }
            System.out.print("\n");
        }

        LocalDate max = LocalDate.MAX;
        System.out.println(max);
        LocalDateTime maxDateTime = LocalDateTime.MAX;
        System.out.println(maxDateTime);

    }

}
