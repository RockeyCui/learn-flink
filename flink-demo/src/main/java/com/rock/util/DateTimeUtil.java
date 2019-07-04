package com.rock.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author RockeyCui
 */
public class DateTimeUtil {

    public final static String DATE_FORMAT_YYYYMMDD = "yyyyMMdd";
    public final static String DATE_FORMAT_YYYY_MM_DD = "yyyy-MM-dd";
    public final static String DATETIME_FORMAT_HHMMSS = "HHmmss";
    public final static String DATETIME_FORMAT_HHMMSSsss = "HHmmssSSS";
    public final static String DATETIME_FORMAT_YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
    public final static String DATETIME_FORMAT_YYYY_MM_DD_HH_MM = "yyyy-MM-dd HH:mm";
    public final static String DATETIME_FORMAT_YYYYMMDDHHMMSS = "yyyyMMddHHmmss";
    public final static String DATETIME_FORMAT_YYYYMMDDHH = "yyyyMMddHH";
    public final static String DATETIME_FORMAT_YYYYMMDDHHMMSSSSS = "yyyyMMddHHmmssSSS";
    public final static String DATETIME_FORMAT_YYYY = "yyyy";
    public final static String DATETIME_FORMAT_YYYY_MM_DD_CN = "yyyy年MM月dd日";
    public final static String DATETIME_FORMAT_HH_MM_SS = "HH:mm:ss";
    public final static String DATE_FORMAT_YYYYPOINTMMPOINTDD = "yyyy.MM.dd";
    public final static String DATETIME_FORMAT_YYYY_MM_DD_HH_MM_SS_SSS = "yyyy-MM-dd HH:mm:ss,SSS";
    public final static String DATE_FORMAT_YYMMDD = "yyMMdd";

    /**
     * 获得指定日期的后N天
     *
     * @param date,num
     * @return
     */
    public static Date getSpecifiedDayAfter(Date date, int num) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        int day = c.get(Calendar.DATE);
        c.set(Calendar.DATE, day + num);
        return c.getTime();
    }

    /**
     * 日期间隔是否大于指定数
     *
     * @param begin
     * @param end
     * @param timeinmill
     * @return
     */
    public static boolean isAfterInMill(Date begin, Date end, long timeinmill) {
        long begininmill = begin.getTime();
        long endinmill = end.getTime();
        if (endinmill - begininmill > timeinmill) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 比较两个时间大小
     * wangbo 2009.5.5
     *
     * @param first
     * @param second
     * @return <0: first<second
     * =0: first=second
     * >0: first>second
     */
    public static int compareTwoDate(Date first, Date second) {
        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();

        c1.setTime(first);
        c2.setTime(second);

        return c1.compareTo(c2);
    }

    /**
     * 取得当前日期所在周的第一天
     *
     * @param date
     * @return
     */
    public static Date getFirstDayOfWeek(Date date) {
        Calendar c = new GregorianCalendar();
        c.setFirstDayOfWeek(Calendar.MONDAY);
        c.setTime(date);
        // Monday
        c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek());
        return c.getTime();
    }

    /**
     * 取得当前日期所在周的最后一天
     *
     * @param date
     * @return
     */
    public static Date getLastDayOfWeek(Date date) {
        Calendar c = new GregorianCalendar();
        c.setFirstDayOfWeek(Calendar.MONDAY);
        c.setTime(date);
        // Sunday
        c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek() + 6);
        return c.getTime();
    }

    /**
     * 计算日期增加或减少小时数后的日期
     *
     * @param date
     * @param i    //为负表示减多少小时
     * @return
     */
    public static Date addHH(Date date, int i) {
        if (date == null) {
            return null;
        }
        Calendar c = new GregorianCalendar();
        c.setTime(date);
        c.add(Calendar.HOUR, i);
        return c.getTime();
    }

    /**
     * 计算日期增加或减少分钟数后的日期
     *
     * @param date
     * @param i    //为负表示减多少分钟
     * @return
     */
    public static Date addMM(Date date, int i) {
        if (date == null) {
            return null;
        }
        Calendar c = new GregorianCalendar();
        c.setTime(date);
        c.add(Calendar.MINUTE, i);
        return c.getTime();
    }

    /**
     * 计算日期增加或减少秒数后的日期
     *
     * @param date
     * @param i    //为负表示减多少秒
     * @return
     */
    public static Date addSS(Date date, int i) {
        if (date == null) {
            return null;
        }
        Calendar c = new GregorianCalendar();
        c.setTime(date);
        c.add(Calendar.SECOND, i);
        return c.getTime();
    }

    /**
     * 计算日期增加减少天数后的日期
     *
     * @param date
     * @param i    //为负表示减多少天
     * @return
     */
    public static Date addDate(Date date, int i) {
        if (date == null) {
            return null;
        }
        Calendar c = new GregorianCalendar();
        c.setTime(date);
        c.add(Calendar.DATE, i);
        return c.getTime();
    }

    /**
     * 计算日期增加减少月数后的日期
     *
     * @param date
     * @param i    //为负表示减多少月
     * @return
     */
    public static Date addMonth(Date date, int i) {
        if (date == null) {
            return null;
        }
        Calendar c = new GregorianCalendar();
        c.setTime(date);
        c.add(Calendar.MONTH, i);
        return c.getTime();
    }

    /**
     * 计算日期增加减少年数后的日期
     *
     * @param date
     * @param i    //为负表示减多少年
     * @return
     */
    public static Date addYear(Date date, int i) {
        if (date == null) {
            return null;
        }
        Calendar c = new GregorianCalendar();
        c.setTime(date);
        c.add(Calendar.YEAR, i);
        return c.getTime();
    }


    /**
     * 获得当前时间字符串
     *
     * @param formatStr 日期格式
     * @return string yyyy-MM-dd
     */
    public static String getNowDateStr(String formatStr) {
        SimpleDateFormat format = new SimpleDateFormat(formatStr);
        return format.format(getNowDate());
    }

    /**
     * 获得系统当前时间
     *
     * @return Date
     */
    public static Date getNowDate() {
        Calendar c = Calendar.getInstance();
        return c.getTime();
    }

    /**
     * 获得系统当前时间
     *
     * @return Date
     */
    public static Date getNowDate(String formatStr) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat(formatStr);
        Calendar c = Calendar.getInstance();
        return format.parse(format.format(c.getTime()));
    }

    /**
     * 把日期按照指定格式的转化成字符串
     *
     * @param date      日期对象
     * @param formatStr 日期格式
     * @return 字符串式的日期, 格式为：yyyy-MM-dd HH:mm:ss
     */
    public static String getDateTimeToString(Date date, String formatStr) {
        SimpleDateFormat format = new SimpleDateFormat(formatStr);
        return format.format(date);
    }

    /**
     * 把日期字符串转化成指定格式的日期对象
     *
     * @param dateStr   日期字符串
     * @param formatStr 日期格式
     * @return Date类型的日期
     * @throws Exception
     */
    public static Date getStringToDateTime(String dateStr, String formatStr) throws Exception {
        SimpleDateFormat format = new SimpleDateFormat(formatStr);
        return format.parse(dateStr);
    }

    /**
     * 校验日期与格式是否一致
     *
     * @param dttm
     * @param format
     * @return
     */
    public static boolean isDate(String dttm, String format) {
        if (dttm == null || dttm.isEmpty() || format == null || format.isEmpty()) {
            return false;
        }

        if (format.replaceAll("'.+?'", "").indexOf("y") < 0) {
            format += "/yyyy";
            DateFormat formatter = new SimpleDateFormat("/yyyy");
            dttm += formatter.format(new Date());
        }

        DateFormat formatter = new SimpleDateFormat(format);
        formatter.setLenient(false);
        ParsePosition pos = new ParsePosition(0);
        Date date = formatter.parse(dttm, pos);

        if (date == null || pos.getErrorIndex() > 0) {
            return false;
        }
        if (pos.getIndex() != dttm.length()) {
            return false;
        }

        if (formatter.getCalendar().get(Calendar.YEAR) > 9999) {
            return false;
        }

        return true;
    }


    /**
     * 获得当前时间的i分钟后（或前，用负数表示）的时间
     *
     * @param i
     * @return
     */
    public static String addMM(int i) {
        Date currTime = addMM(getNowDate(), i);
        SimpleDateFormat format = new SimpleDateFormat(DATETIME_FORMAT_YYYYMMDDHHMMSS);
        return format.format(currTime);
    }

    /**
     * 获得某个时间的i分钟后（或前，用负数表示）的时间
     *
     * @param i
     * @return
     */
    public static String dateAddMM(Date date, int i) {
        Date currTime = addMM(date, i);
        SimpleDateFormat format = new SimpleDateFormat(DATETIME_FORMAT_YYYYMMDDHHMMSS);
        return format.format(currTime);
    }


    /**
     * @param date 获取给定日期的起初时间 XX-XX-XX 00:00:00
     * @return date
     */
    public static Date getBegin(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar.getTime();
    }

    /**
     * @param date 获取给定日期的结束时间 XX-XX-XX 23:59:59
     * @return date
     */
    public static Date getEnd(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        return calendar.getTime();
    }

    public static boolean isBeforeToday(Date theDay) {
        Calendar cNow = Calendar.getInstance();
        int iYear = cNow.get(Calendar.YEAR);
        int iDay = iYear * 1000 + cNow.get(Calendar.DAY_OF_YEAR);

        Calendar cDay = Calendar.getInstance();
        cDay.setTime(theDay);
        int iTheYear = cDay.get(Calendar.YEAR);
        int iTheDay = iTheYear * 1000 + cDay.get(Calendar.DAY_OF_YEAR);

        return iTheDay < iDay;
    }

    public static boolean isToday(Date theDay) {
        Calendar cNow = Calendar.getInstance();
        int iYear = cNow.get(Calendar.YEAR);
        int iDay = cNow.get(Calendar.DAY_OF_YEAR);

        Calendar cDay = Calendar.getInstance();
        cDay.setTime(theDay);
        int iTheYear = cDay.get(Calendar.YEAR);
        int iTheDay = cDay.get(Calendar.DAY_OF_YEAR);

        return (iTheYear == iYear) && (iTheDay == iDay);
    }

    /**
     * 计算两个日期之间的相差的天数. 计算方式：second - first
     * <p> Create Date: 2015年1月22日 </p>
     *
     * @param smdate 较小的时间
     * @param bdate  较大的时间
     * @return 相差的天数
     */
    public static int daysBetween(Date smdate, Date bdate) {
        int result = 0;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            smdate = sdf.parse(sdf.format(smdate));
            bdate = sdf.parse(sdf.format(bdate));
            Calendar cal = Calendar.getInstance();
            cal.setTime(smdate);
            long time1 = cal.getTimeInMillis();
            cal.setTime(bdate);
            long time2 = cal.getTimeInMillis();
            long betweenDays = (time2 - time1) / (1000 * 3600 * 24);
            result = Integer.parseInt(String.valueOf(betweenDays));
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * 计算两个日期之间的相差的秒数. 计算方式：second - first
     *
     * @param smdate 较小的时间
     * @param bdate  较大的时间
     * @return 相差的秒数
     */
    public static int secondsBetween(Date smdate, Date bdate) {
        int result = 0;
        try {
            Calendar cal = Calendar.getInstance();
            cal.setTime(smdate);
            long time1 = cal.getTimeInMillis();
            cal.setTime(bdate);
            long time2 = cal.getTimeInMillis();
            long betweenMin = (time2 - time1) / 1000;
            result = Integer.parseInt(String.valueOf(betweenMin));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * 将时间戳转换为date
     * <p> Create Date: 2015年3月30日 </p>
     *
     * @param seconds
     * @return
     */
    public static Date getTimestampToDate(long seconds) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(seconds);

        return calendar.getTime();
    }

    /**
     * 获取系统当前日期的前一天
     *
     * @return Date
     */
    public static Date beforeCurrentDate(String formatStr) {
        Date ret = null;
        SimpleDateFormat formatter = new SimpleDateFormat(formatStr);
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.DAY_OF_MONTH, -1);
            ret = formatter.parse(formatter.format(calendar.getTime()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    public static List<Date> findDates(Date dStart, Date dEnd) {
        Calendar cStart = Calendar.getInstance();
        cStart.setTime(dStart);
        List<Date> dateList = new ArrayList<>();
        dateList.add(dStart);
        // 此日期是否在指定日期之后
        while (dEnd.after(cStart.getTime())) {
            // 根据日历的规则，为给定的日历字段添加或减去指定的时间量
            cStart.add(Calendar.DAY_OF_MONTH, 1);
            dateList.add(cStart.getTime());
        }
        return dateList;
    }
}
