package com.taobao.tddl.monitor.stat;

import java.util.Date;

import org.apache.commons.lang.time.FastDateFormat;

import com.taobao.tddl.common.utils.logger.Logger;

/**
 * 基于 Tddl Logger 的统计日志输出类。 保证向下兼容, 日志刷出的结构为：
 * 
 * <pre>
 * key1(sql)    key2(dbname)    key3(flag)  val1(count) val2(sum)       val3(min)       val4(max)       time
 * sql      logicDbName 执行成功        执行次数        响应时间        最小响应时间  最大响应时间  日志时间
 * sql      realDbName1 执行成功        执行次数        响应时间        最小响应时间  最大响应时间  日志时间
 * sql      realDbName2 执行成功        执行次数        响应时间        最小响应时间  最大响应时间  日志时间
 * sql      realDbName2 执行失败        执行次数        响应时间        最小响应时间  最大响应时间  日志时间
 * sql      realDbName2 执行超时        执行次数        响应时间        最小响应时间  最大响应时间  日志时间
 * sql      null        解析成功        执行次数        响应时间        最小响应时间  最大响应时间  日志时间
 * sql      null        解析失败        执行次数        响应时间        最小响应时间  最大响应时间  日志时间
 * sql      null        解析命中        执行次数        命中次数        NA      NA      日志时间
 * </pre>
 * 
 * @author changyuan.lh
 */
public class LoggerLogWriter extends StatLogWriter {

    /** XXX: 改成 commons-lang 自带的 {@link FastDateFormat}, 这个才是线程安全的 */
    public static final FastDateFormat df             = FastDateFormat.getInstance("yyy-MM-dd HH:mm:ss:SSS");

    protected String                   fieldSeperator = "#@#";                                               // SQL中出现概率小,
                                                                                                              // 和正则式不冲突
    protected String                   lineSeperator  = System.getProperty("line.separator");
    protected final Logger             statLogger;

    public LoggerLogWriter(Logger statLogger){
        this.statLogger = statLogger;
    }

    public LoggerLogWriter(String fieldSeperator, Logger statLogger){
        this.fieldSeperator = fieldSeperator;
        this.statLogger = statLogger;
    }

    public LoggerLogWriter(String fieldSeperator, String lineSeperator, Logger statLogger){
        this.fieldSeperator = fieldSeperator;
        this.lineSeperator = lineSeperator;
        this.statLogger = statLogger;
    }

    // XXX: 输出中首先写信息, 然后写数据, 最后写时间, 保持向后兼容
    protected StringBuffer format(StringBuffer buf, Object[] fields, Date time, long... values) {
        for (Object field : fields) {
            buf.append(field).append(fieldSeperator);
        }
        for (long value : values) {
            buf.append(value).append(fieldSeperator);
        }
        return buf.append(df.format(time)).append(lineSeperator);
    }

    public void write(Object[] keys, Object[] fields, long... values) {
        StringBuffer buf = new StringBuffer();
        format(buf, (fields == null) ? keys : fields, new Date(), values);
        statLogger.warn(buf.toString());
    }
}
