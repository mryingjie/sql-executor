package com.heitaox.sql.executor.core.entity;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

/**
 * The parsing condition obtained after parsing the having, where, and on clauses in SQL
 */
@Data
@NoArgsConstructor
public class PredicateEntity<T> {

    /**
     * 字段一
     */
    private String field;

    /**
     * 如果是in筛选器将会有in 和 not in的区别
     */
    private Boolean not;

    /**
     * > < >= <= != like = 等符号
     */
    private SQLBinaryOperator predicateSymbol;

    /**
     * and 和 or
     */
    private SQLBinaryOperator connecSymbol;

    /**
     * 可能是字段2 也可能是某个值
     */
    private T value;

    /**
     * 括号
     */
    private String brackets;

    /**
     * 左括号
     */
    public static final PredicateEntity leftBrackets;

    /**
     * 右括号
     */
    public static final PredicateEntity rightBrackets;

    static {
        leftBrackets = new PredicateEntity();
        leftBrackets.setBrackets("(");
        rightBrackets = new PredicateEntity();
        rightBrackets.setBrackets(")");
    }

    public PredicateEntity(SQLBinaryOperator connecSymbol) {
        this.connecSymbol = connecSymbol;
    }

    @Override
    public String toString() {
        return "PredicateEntity{" +
                "field='" + field + '\'' +
                ", not=" + not +
                ", predicateSymbol=" + predicateSymbol +
                ", connecSymbol=" + connecSymbol +
                ", value=" + value +
                ", brackets='" + brackets + '\'' +
                '}';
    }

    public String transToString() {
        if (brackets != null) {
            return brackets;
        } else if (field != null) {
            if (not != null) {
                StringBuilder sb = new StringBuilder();
                sb.append(field).append(" ");
                if (not) {
                    sb.append("not").append(" ").append("in");
                } else {
                    sb.append("in");
                }
                sb.append("(");
                for (Object o : ((List) value)) {
                    if (o instanceof String) {
                        sb.append("'").append(o).append("'").append(",");
                    } else if (o instanceof BigDecimal) {
                        sb.append(((BigDecimal) o).doubleValue()).append(",");
                    } else {
                        sb.append(o).append(",");
                    }
                }
                sb.delete(sb.lastIndexOf(","), sb.length());
                sb.append(")");
                return sb.toString();
            } else {
                return field + " " + predicateSymbol.name + " " + "'"+value.toString()+"'";
            }
        } else {
            return connecSymbol.name;
        }

    }
}
