package com.heitaox.sql.executor.source.rdbms;


import lombok.Data;


@Data
public class RDBMSDataSourceProperties {

    private volatile String username;
    private volatile String password;
    private volatile String url;
    private volatile String driverClass;
    private volatile int initialSize = 0;
    private volatile int maxActive = 8;
    private volatile int minIdle = 0;
    private volatile int maxIdle = 8;
    private volatile long maxWait = -1L;
    private volatile int validationQueryTimeout;
    private volatile boolean testOnBorrow;
    private volatile boolean testOnReturn;
    private volatile boolean testWhileIdle;

    private volatile int maxWaitThreadCount;
    private volatile boolean accessToUnderlyingConnectionAllowed;
    private volatile String validationQuery;

    private volatile long timeBetweenEvictionRunsMillis;
    private volatile int numTestsPerEvictionRun;
    private volatile long minEvictableIdleTimeMillis;
    private volatile boolean removeAbandoned;
    private volatile long removeAbandonedTimeout;
    private volatile boolean logAbandoned;
    private volatile int maxOpenPreparedStatements;
    private volatile String dbType;
    private volatile long timeBetweenConnectErrorMillis;

    private volatile boolean poolPreparedStatements;
    private volatile boolean sharePreparedStatements;
    private volatile int maxPoolPreparedStatementPerConnectionSize;


}
