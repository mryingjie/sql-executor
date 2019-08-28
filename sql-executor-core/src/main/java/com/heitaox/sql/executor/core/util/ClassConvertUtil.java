package com.heitaox.sql.executor.core.util;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClassConvertUtil {
    public static Object convertClass(Class typeCla, Object o) {
        if (o == null){
            return null;
        }
        if (typeCla == Object.class) {
            return o;
        }
        if (typeCla == Number.class) {
            typeCla = Long.class;
        }
        try {
            String v = o.toString();

            if (!typeCla.isPrimitive()) { // 判断基本类型
                if (typeCla.equals(String.class)) { // 如果是string则直接返回
                    return v;
                }
                //  如果不为null 则通过反射实例一个对象返回

                return "".equals(v) ? null : typeCla.getConstructor(String.class).newInstance(v);
            }

            // 下面处理基本类型，返回包装类
            String name = typeCla.getName();
            switch (name) {
                case "String":
                    return v;
                case "int":
                    return Integer.parseInt(v);
                case "byte":
                    return Byte.parseByte(v);
                case "boolean":
                    return Boolean.parseBoolean(v);
                case "double":
                    return Double.parseDouble(v);
                case "float":
                    return Float.parseFloat(v);
                case "long":
                    return Long.parseLong(v);
                case "short":
                    return Short.parseShort(v);
                default:
                    return v;
            }
        } catch (Exception e) {
            if (!typeCla.equals(Double.class)) {
                return convertClass(Double.class, o);
            } else {
                return convertClass(Object.class, o);
            }
        }

    }
}
