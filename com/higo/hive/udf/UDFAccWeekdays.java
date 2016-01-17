package com.higo.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.util.Date;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.text.ParseException;

/**
 * UDFAccWeekdays
 *
 */
@Description(
    name = "accweekdays",
    value = "_FUNC_(start_ts, end_ts) - returns the weekday period(unit:second) that starts at start_ts and ends at end_ts",
    extended = "both start_ts and end_ts are unix_timestamp.\n" +
        "Example:\n" +
        "   > SELECT _FUNC_(1452149997, 1452150015) FROM src LIMIT 1;\n" +
        "   18 \n"
    )
public class UDFAccWeekdays extends UDF {

    private final LongWritable r;
    private static int DAYS_COUNT_IN_A_WEEK = 7;
    private static int WEEKENDS_COUNT_IN_A_WEEK = 2;
    private static int SECONDS_COUNT_IN_A_DAY = 86400; // 60 * 60 * 24
    private static int START_DATE = 1;
    private static int END_DATE = 2;

    public UDFAccWeekdays() {
        r = new LongWritable();
    }

    public LongWritable evaluate(LongWritable start_ts, LongWritable end_ts) {
        if (start_ts == null || end_ts == null || start_ts.get() > end_ts.get()) {
            return null;
        }
        
        r.set(this.calculate(start_ts.get(), end_ts.get()));

        return r;
    }

    private long calculate(long st, long end) {

        if (st > end) {
            throw new IllegalArgumentException("the start date is after the end date");
        }

        Date startD = new Date(st * 1000);
        Date endD = new Date(end * 1000);

        startD = this.convertDate(startD, UDFAccWeekdays.START_DATE);
        endD = this.convertDate(endD, UDFAccWeekdays.END_DATE);

        if (startD.after(endD)) { //st和end在同一个周末
            return 0;
        }

        Calendar startC = Calendar.getInstance();
        startC.setTime(startD);
        Calendar endC = Calendar.getInstance();
        endC.setTime(endD);

        //day of week
        int stDayOfWeek = startC.get(Calendar.DAY_OF_WEEK);
        int endDayOfWeek = endC.get(Calendar.DAY_OF_WEEK);

        //两个时间所差unix_timestamp
        long totalPeriodTs = (endD.getTime() - startD.getTime()) / 1000;
        //天数相差
        int days = (int) totalPeriodTs / UDFAccWeekdays.SECONDS_COUNT_IN_A_DAY;
        //周数相差
        int weeks = days / UDFAccWeekdays.DAYS_COUNT_IN_A_WEEK;

        //返回值
        long workPeriodTs = 0;

        if (stDayOfWeek <= endDayOfWeek) {
            workPeriodTs = totalPeriodTs - UDFAccWeekdays.SECONDS_COUNT_IN_A_DAY * WEEKENDS_COUNT_IN_A_WEEK * weeks;
        } else {
            workPeriodTs = totalPeriodTs - UDFAccWeekdays.SECONDS_COUNT_IN_A_DAY * (weeks + 1) * UDFAccWeekdays.WEEKENDS_COUNT_IN_A_WEEK;
        }

        return workPeriodTs;
    }

    /**
     * 获取开始计算的时间
     */
    private Date convertDate(Date dt, int type) {
        if (dt == null) {
            return null;
        }

        Calendar cal = Calendar.getInstance();
        cal.setTime(dt);
        int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");

        long ts = 0;
        try {
            Date date1;
            date1 = ft.parse(ft.format(dt));
            ts = date1.getTime();
        } catch(ParseException e) {
            e.printStackTrace();
        }

        Date res;
        if (dayOfWeek == 1) { //周日
            if (UDFAccWeekdays.START_DATE == type) {
                res = new Date(ts + UDFAccWeekdays.SECONDS_COUNT_IN_A_DAY * 1000);
            } else {
                res = new Date(ts - UDFAccWeekdays.SECONDS_COUNT_IN_A_DAY * 1000);
            }
        } else if (dayOfWeek == 7) { //周六
            if (UDFAccWeekdays.START_DATE == type) {
                res = new Date(ts + UDFAccWeekdays.SECONDS_COUNT_IN_A_DAY * 1000 * 2);
            } else {
                res = new Date(ts);
            }
        } else { //工作日
            res = dt;
        }

        return res;
    }
}
