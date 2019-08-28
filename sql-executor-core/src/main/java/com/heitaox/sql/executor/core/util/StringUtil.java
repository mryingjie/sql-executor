package com.heitaox.sql.executor.core.util;


public class StringUtil {

    public static int compare(String s1, String s2) {
        if(s1 == null){
            s1 = "";
        }
        if(s2 == null){
            s2 = "";
        }
        if (s1.equals(s2)) {
            return 0;
        }
        Double d1;
        Double d2;
        try {
            d1 = Double.parseDouble(s1);
            d2 = Double.parseDouble(s2);
            return d1.compareTo(d2);
        } catch (NumberFormatException e) {
            return s1.compareTo(s2);
        }
    }



}
