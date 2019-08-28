package com.heitaox.sql.executor.source.nosql;

import com.mongodb.ServerAddress;
import lombok.Data;

import java.util.List;

@Data
public class MongoDataSourceProperties {

    /**
     * 用户名
     */
    private String user;

    /**
     * 密码
     */
    private char[] password;


    /**
     * 数据库名
     */
    private String dbName;

    /**
     * 地址
     */
    private List<ServerAddress> serverAddress;


}
