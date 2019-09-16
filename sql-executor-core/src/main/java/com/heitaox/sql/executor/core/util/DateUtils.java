package com.heitaox.sql.executor.core.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;

/**
 * 日期时间工具类
 */
public class DateUtils {

    public static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static final DateTimeFormatter yyyyMMdd = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter yyyyMMddHHmm = DateTimeFormatter.ofPattern("yyyy-MM-ddHHmm");

    public static final ZoneOffset ZONE_OFF_SET = ZoneOffset.of("+8");

    /**
     * 判断一个时间是否在另一个时间之前
     *
     * @param time1 第一个时间
     * @param time2 第二个时间
     * @return 判断结果
     */
    public static boolean before(String time1, String time2) {
        LocalDateTime parse1 = LocalDateTime.parse(time1, dateTimeFormatter);
        LocalDateTime parse2 = LocalDateTime.parse(time2, dateTimeFormatter);

        return parse1.isBefore(parse2);
    }

    /**
     * 判断一个时间是否在另一个时间之后
     *
     * @param time1 第一个时间
     * @param time2 第二个时间
     * @return 判断结果
     */
    public static boolean after(String time1, String time2) {
        LocalDateTime parse1 = LocalDateTime.parse(time1, dateTimeFormatter);
        LocalDateTime parse2 = LocalDateTime.parse(time2, dateTimeFormatter);

        return parse1.isAfter(parse2);
    }

    /**
     * 计算时间差值（单位为秒）
     *
     * @param time1 时间1
     * @param time2 时间2
     * @return 差值 绝对值
     */
    public static Long minus(String time1, String time2) {
        LocalDateTime parse1 = LocalDateTime.parse(time1, dateTimeFormatter);
        LocalDateTime parse2 = LocalDateTime.parse(time2, dateTimeFormatter);


        //获取秒数
        Long second1 = parse1.toEpochSecond(ZONE_OFF_SET);

        //获取毫秒数
//            Long milliSecond = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        Long second2 = parse2.toEpochSecond(ZONE_OFF_SET);

        return Math.abs(second1 - second2);
    }

    /**
     * 获取年月日和小时
     *
     * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
     * @return 结果（yyyyMMddHH）
     */
    public static String getDateHour(String datetime) {
        LocalDateTime parse1 = LocalDateTime.parse(datetime, dateTimeFormatter);
        return datetime.substring(0,8)+datetime.charAt(9)+datetime.charAt(10);
    }

    /**
     * 获取年月日和小时
     *
     * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
     * @return 结果（yyyyMMddHH）
     */
    public static String getDateHour(LocalDateTime datetime) {
        return datetime.getYear()+""+
                StringUtils.fulfuill(String.valueOf(datetime.getMonthValue()))+
                StringUtils.fulfuill(String.valueOf(datetime.getDayOfMonth()))+
                StringUtils.fulfuill(String.valueOf(datetime.getHour()));
    }

    /**
     * 获取当天日期（yyyy-MM-dd）
     *
     * @return 当天日期
     */
    public static String getTodayDate() {
        return LocalDate.now().format(yyyyMMdd);
    }

    /**
     * 获取昨天的日期（yyyy-MM-dd）
     *
     * @return 昨天的日期
     */
    public static String getYesterdayDate() {
        LocalDate plus = LocalDate.now().plus(-1, ChronoUnit.DAYS);


        return plus.format(yyyyMMdd);
    }
    /**
     * 获取昨天的日期（yyyy-MM-dd）
     * @param date 今天的日期
     * @return 昨天的日期
     */
    public static String getYesterdayDate(String date) {
        LocalDate parse = LocalDate.parse(date, yyyyMMdd);
        LocalDate plus = parse.plus(-1, ChronoUnit.DAYS);

        return plus.format(yyyyMMdd);
    }

    /**
     * 获取明天的日期（yyyy-MM-dd）
     * @param date 今天的日期
     * @return 明天的日期
     */
    public static String getAfterdayDate(String date) {
        LocalDate parse = LocalDate.parse(date, yyyyMMdd);
        LocalDate plus = parse.plus(1, ChronoUnit.DAYS);

        return plus.format(yyyyMMdd);
    }



    /**
     * 格式化日期（yyyy-MM-dd）
     *
     * @param date Date对象
     * @return 格式化后的日期
     */
    public static String formatDate(Date date) {
        long time = date.getTime();
        LocalDate localDate = Instant.ofEpochMilli(time).atOffset(ZONE_OFF_SET).toLocalDate();
        return localDate.format(yyyyMMdd);
    }

    /**
     * 格式化时间（yyyy-MM-dd）
     *
     * @param timeMillis 毫秒值
     * @return 格式化后的时间
     */
    public static String formatDate(Long timeMillis) {
        return Instant.ofEpochMilli(timeMillis).atOffset(ZONE_OFF_SET).toLocalDate().format(yyyyMMdd);
    }

    /**
     * 格式化时间（yyyy-MM-dd HH:mm:ss）
     *
     * @param date Date对象
     * @return 格式化后的时间
     */
    public static String formatDateTime(Date date) {
        long time = date.getTime();
        LocalDateTime localDateTime = Instant.ofEpochMilli(time).atOffset(ZONE_OFF_SET).toLocalDateTime();
        return localDateTime.format(dateTimeFormatter);
    }

    /**
     * 格式化时间（yyyy-MM-dd HH:mm:ss）
     *
     * @param timeMillis 毫秒值
     * @return 格式化后的时间
     */
    public static String formatDateTime(Long timeMillis) {
        return Instant.ofEpochMilli(timeMillis).atOffset(ZONE_OFF_SET).toLocalDateTime().format(dateTimeFormatter);
    }

    /**
     *
     * @param timeMillis 毫秒值
     * @return 格式化后的时间
     */
    public static String formatDateTime(Long timeMillis,DateTimeFormatter dateTimeFormatter) {
        return Instant.ofEpochMilli(timeMillis).atOffset(ZONE_OFF_SET).toLocalDateTime().format(dateTimeFormatter);
    }

    /**
     * 解析时间字符串
     *
     * @param time 时间字符串(yyyy-MM-dd HH:mm:ss)
     * @return LocalDateTime
     */
    public static LocalDateTime parseTime(String time) {
        return LocalDateTime.parse(time, dateTimeFormatter);
    }


    /**
     * 格式化日期
     *@param date 时间字符串(yyyy-MM-dd)
     * @return LocalDate
     */
    public static LocalDate parseDateKey(String date) {
        return LocalDate.parse(date, yyyyMMdd);
    }

    /**
     * 格式化时间，保留到分钟级别
     * yyyy-MM-dd HHmm
     *
     * @param date data
     * @return (yyyy-MM-dd HHmm)
     */
    public static String formatTimeMinute(Date date) {
        long time = date.getTime();
        return formatTimeMinute(time);
    }

    public static String formatTimeMinute(Long timeMillis){
        LocalDateTime localDateTime = Instant.ofEpochMilli(timeMillis).atOffset(ZONE_OFF_SET).toLocalDateTime();
        return localDateTime.format(yyyyMMddHHmm);
    }

    /**
     *
     * @param dateTime dataTime
     * @return ("yyyy-MM-dd HH:mm:ss")
     */
    public static String formatDateTime(LocalDateTime dateTime){
        String format = dateTime.format(dateTimeFormatter);
        return format;
    }

    /**
     * @param dateStr yyyy-MM-dd
     * @return Date
     */
    public static Date formateDateStr(String dateStr){
        if(dateStr==null){
            return null;
        }
        LocalDate parse = LocalDate.parse(dateStr, yyyyMMdd);
        return new Date(parse.getYear(),parse.getMonthValue(),parse.getDayOfMonth());
    }

}
