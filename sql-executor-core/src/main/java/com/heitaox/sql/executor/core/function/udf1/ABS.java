package com.heitaox.sql.executor.core.function.udf1;

/**
 *
 */
public class ABS extends UDF1<Number, Number> {
    @Override
    public Number trans(Number in) {
        if (in == null)
            return null;
        Number res;
        if (in instanceof Double) {
            res = in.doubleValue() > 0 ? in : -in.doubleValue();
        } else if (in instanceof Float) {
            res = in.floatValue() > 0 ? in : -in.floatValue();
        } else if (in instanceof Integer) {
            res = in.intValue() > 0 ? in : -in.intValue();
        } else if (in instanceof Short) {
            res = in.shortValue() > 0 ? in : -in.shortValue();
        } else if (in instanceof Long) {
            res = in.longValue() > 0 ? in : -in.longValue();
        } else {
            res = in.doubleValue() > 0 ? in : -in.doubleValue();
        }
        return res;
    }
}
