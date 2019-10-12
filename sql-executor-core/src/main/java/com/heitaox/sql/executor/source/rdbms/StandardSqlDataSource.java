package com.heitaox.sql.executor.source.rdbms;

import com.alibaba.druid.pool.DruidDataSource;
import com.heitaox.sql.executor.source.RDBMSDataSource;
import joinery.DataFrame;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;


@Data
@Slf4j
public class StandardSqlDataSource implements RDBMSDataSource {

    private DataSource dataSource;

    private RDBMSDataSourceProperties properties;

    public StandardSqlDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public StandardSqlDataSource(RDBMSDataSourceProperties properties) throws Exception {
        DruidDataSource dataSource = new DruidDataSource();

        dataSource.setUrl(properties.getUrl());
        dataSource.setUsername(properties.getUsername());
        dataSource.setPassword(properties.getPassword());
        dataSource.setDriverClassName(properties.getDriverClass());
        dataSource.setInitialSize(properties.getInitialSize());
        dataSource.setTestOnReturn(properties.isTestOnReturn());
        dataSource.setTestOnBorrow(properties.isTestOnBorrow());
        dataSource.setMinIdle(properties.getMinIdle());
        dataSource.setMaxActive(properties.getMaxActive());
        dataSource.setMaxWait(properties.getMaxWait());
        // 配置间隔多久启动一次DestroyThread，对连接池内的连接才进行一次检测，单位是毫秒。
        // 检测时:1.如果连接空闲并且超过minIdle以外的连接，如果空闲时间超过minEvictableIdleTimeMillis设置的值则直接物理关闭。2.在minIdle以内的不处理。
        dataSource.setTimeBetweenEvictionRunsMillis(properties.getTimeBetweenEvictionRunsMillis());
        // 配置一个连接在池中最大空闲时间，单位是毫秒
        dataSource.setMinEvictableIdleTimeMillis(properties.getMinEvictableIdleTimeMillis() == 0 ? 40000L : properties.getMinEvictableIdleTimeMillis());
        // 检验连接是否有效的查询语句。如果数据库Driver支持ping()方法，则优先使用ping()方法进行检查，否则使用validationQuery查询进行检查。(Oracle jdbc Driver目前不支持ping方法)
        dataSource.setValidationQuery(properties.getValidationQuery());

        // 连接泄露检查，打开removeAbandoned功能 , 连接从连接池借出后，长时间不归还，将触发强制回连接。回收周期随timeBetweenEvictionRunsMillis进行，如果连接为从连接池借出状态，并且未执行任何sql，并且从借出时间起已超过removeAbandonedTimeout时间，则强制归还连接到连接池中。
        dataSource.setRemoveAbandoned(properties.isRemoveAbandoned());
        // 超时时间，秒
        dataSource.setRemoveAbandonedTimeout(properties.getValidationQueryTimeout());

        dataSource.setMaxPoolPreparedStatementPerConnectionSize(properties.getMaxPoolPreparedStatementPerConnectionSize());
        // 打开PSCache，并且指定每个连接上PSCache的大小，Oracle等支持游标的数据库，打开此开关，会以数量级提升性能，具体查阅PSCache相关资料
        dataSource.setPoolPreparedStatements(properties.isPoolPreparedStatements());
        dataSource.setBreakAfterAcquireFailure(true);
        try {
            dataSource.init();
            this.dataSource = dataSource;
        } catch (Exception e) {
            log.error("dataSource init failed", e);
            throw e;
        }


    }


    @Override
    public DataFrame<Object> executeQuery(String sql) {
        QueryRunner queryRunner = new QueryRunner(this.dataSource);
        List<Map<String, Object>> queryResult = null;
        DataFrame<Object> df = null;
        try {
            queryResult = queryRunner.query(sql, new MapListHandler());
            if (queryResult != null || queryResult.size() > 0) {
                Set<String> columns = queryResult.get(0).keySet();
                df = new DataFrame<>(columns);
                for (Map<String, Object> map : queryResult) {
                    df.append(new ArrayList<>(map.values()));
                }
            }
        } catch (SQLException e) {
            log.error("sql:[" + sql + "] execute failed", e);
        }

        return df == null ? new DataFrame<>() : df;
    }


    public <T> List<T> executeQuery(String sql, Class<T> resClass) {
        QueryRunner queryRunner = new QueryRunner(this.dataSource);
        List<T> queryResult = null;
        try {
            queryResult = queryRunner.query(sql, new BeanListHandler<>(resClass));
        } catch (SQLException e) {
            log.error("sql:[" + sql + "] execute failed", e);
        }
        return queryResult;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        int i = 0;
        Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        i = preparedStatement.executeUpdate();
        return i;
    }


}
