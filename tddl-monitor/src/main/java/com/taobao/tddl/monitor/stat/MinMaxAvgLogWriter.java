package com.taobao.tddl.monitor.stat;

import java.util.Date;

import com.taobao.tddl.common.utils.logger.Logger;

/**
 * 扩展 Log4j Logger 的统计日志输出类, 唯一的区别是在输出 count/sum/min/max 后增加平均值。
 * 
 * @author changyuan.lh
 */
public class MinMaxAvgLogWriter extends LoggerLogWriter {

    public MinMaxAvgLogWriter(Logger statLogger){
        super(statLogger);
    }

    public MinMaxAvgLogWriter(String fieldSeperator, Logger statLogger){
        super(fieldSeperator, statLogger);
    }

    public MinMaxAvgLogWriter(String fieldSeperator, String lineSeperator, Logger statLogger){
        super(fieldSeperator, lineSeperator, statLogger);
    }

    protected StringBuffer format(StringBuffer buf, Object[] fields, Date time, long... values) {
        if (values.length < 2) {
            throw new IllegalArgumentException("At least given 2 values");
        }
        String valueAvgStr = "invalid";
        final long number = values[1];
        final long count = values[0];
        if (count != 0) {
            final long valueAvg = 100 * number / count;
            valueAvgStr = String.valueOf((double) valueAvg / 100);
        }
        for (Object field : fields) {
            buf.append(field).append(fieldSeperator);
        }
        for (long value : values) {
            buf.append(value).append(fieldSeperator);
        }
        buf.append(valueAvgStr).append(fieldSeperator);
        return buf.append(df.format(time)).append(lineSeperator);
    }
}
