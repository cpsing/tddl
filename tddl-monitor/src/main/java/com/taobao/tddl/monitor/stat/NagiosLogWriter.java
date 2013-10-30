package com.taobao.tddl.monitor.stat;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.monitor.utils.NagiosUtils;

/**
 * 基于 Nagios 格式的统计日志输出类。 <br />
 * 兼容 StatMonitor 的代码。
 * 
 * @author changyuan.lh
 */
public class NagiosLogWriter extends StatLogWriter {

    public void write(Object[] keys, Object[] fields, long... values) {
        if (values.length < 2) {
            throw new IllegalArgumentException("At least given 2 values");
        }
        // XXX: 兼容 StatMonitor 的输出, 放弃 min/max 只输出平均值
        long count = values[0];
        long value = values[1];
        String averageValueStr = "invalid";
        if (count != 0) {
            double averageValue = (double) value / count;
            averageValueStr = String.valueOf(averageValue);
        }
        if (fields == null) {
            fields = keys;
        }
        // NagiosUtils.addNagiosLog(key1 + "|" + key2 + "|" + key3,
        // averageValueStr);
        NagiosUtils.addNagiosLog(TStringUtil.join(fields, "|"), averageValueStr);
    }
}
