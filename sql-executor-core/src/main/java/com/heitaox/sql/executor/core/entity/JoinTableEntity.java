package com.heitaox.sql.executor.core.entity;

import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import lombok.Data;

import java.util.List;

/**
 * Parsing the data type obtained by the join clause in sql
 */
@Data
public class JoinTableEntity {

    /**
     * 表名
     */
    private String tName;

    /**
     * 表别名
     */
    private String tAlias;

    /**
     * join类型
     */
    private SQLJoinTableSource.JoinType joinType;

    /**
     * join条件
     */
    private List<PredicateEntity<Object>> conditions;


    public JoinTableEntity(String tAlias,String tName){
        this.tAlias = tAlias;
        this.tName = tName;
    }

    public JoinTableEntity(SQLJoinTableSource.JoinType joinType,List<PredicateEntity<Object>> conditions){
        this.joinType = joinType;
        this.conditions = conditions;
    }

}
