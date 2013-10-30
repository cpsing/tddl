package com.taobao.tddl.monitor;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.monitor.logger.LoggerInit;
import com.taobao.tddl.monitor.stat.BufferedLogWriter;
import com.taobao.tddl.monitor.stat.LoggerLogWriter;
import com.taobao.tddl.monitor.stat.NagiosLogWriter;
import com.taobao.tddl.monitor.stat.StatLogWriter;
import com.taobao.tddl.monitor.utils.NagiosUtils;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * StatMonitor 改为在公共的 BufferedLogWriter 基础上实现。 <br/>
 * 
 * @author changyuan.lh
 * @author guangxia
 * @since 1.0, 2010-2-8 下午04:18:39
 */
public class StatMonitor extends BufferedLogWriter implements StatMonitorMBean {

    private static final Logger      logger   = LoggerFactory.getLogger(StatMonitor.class);

    private static final StatMonitor instance = new StatMonitor();

    private StatMonitor(){
        // XXX: 日志内容是行复制和 SQL 解析, Key 量与 SQL 相同数量级
        super(5 * 60, 1000, 4000, new NagiosLogWriter());
        lastStatMap = map;
    }

    public static StatMonitor getInstance() {
        return instance;
    }

    private volatile ConcurrentHashMap<LogKey, LogCounter> lastStatMap;

    private volatile long                                  lastResetTime = System.currentTimeMillis();
    private volatile long                                  duration      = 0;

    public void resetStat() {
        // XXX: 如果正在刷出, 则不重置
        if (!flushing && flushLock.tryLock()) {
            try {
                lastStatMap = map;
                map = new ConcurrentHashMap<LogKey, LogCounter>( // NL
                maxKeySize,
                    0.75f,
                    32);
                duration = System.currentTimeMillis() - lastResetTime;
                lastResetTime = System.currentTimeMillis();
            } finally {
                flushLock.unlock();
            }
        }
    }

    protected void insureMaxSize() {
        // XXX: StatMonitor 不会刷出 LRU 日志
        // super.insureMaxSize();
    }

    protected void flushAll() {
        try {
            lastStatMap = map;
            duration = System.currentTimeMillis() - lastResetTime;
            lastResetTime = System.currentTimeMillis();
            // 刷出所有统计日志
            super.flushAll();
            // 拉取自定义日志内容并打印
            writeCallBackLog();
        } catch (Throwable e) {
            logger.warn("flushAll", e);
        }
    }

    protected void flushLRU() {
        // XXX: StatMonitor 不会刷出 LRU 日志
        // super.flushLRU();
    }

    public String getStatResult(String key1, String key2, String key3) {
        LogKey logKey = new LogKey(new Object[] { key1, key2, key3 });
        LogCounter counter = lastStatMap.get(logKey);
        if (counter == null) {
            logger.warn("getLastStatResult(" + key1 + ", " + key2 + ", " + key3 + ") Invalid");
            return null;
        }
        long count = counter.getCount();
        long values = counter.getValue();
        String averageValueStr = "invalid";
        String averageCountStr = "invalid";
        if (count != 0) {
            double averageValue = (double) values / count;
            averageValueStr = String.valueOf(averageValue);
        }
        long duration = this.duration;
        if (duration == 0) {
            duration = System.currentTimeMillis() - lastResetTime;
        }
        if (duration != 0) {
            double averageCount = (double) (count * 1000) / duration;
            averageCountStr = String.valueOf(averageCount);
        }
        return "count: " + count + ", value: " + values + ", average: " + averageValueStr + ", Count/Duration: "
               + averageCountStr;
    }

    public long getDuration() {
        return (duration != 0) ? duration : (System.currentTimeMillis() - lastResetTime);
    }

    public long getStatDuration() {
        return lastResetTime;
    }

    public final boolean addStat(String keyOne, String keyTwo, String keyThree) {
        return realTimeStat(keyOne, keyTwo, keyThree, 0);
    }

    public final boolean addStat(String keyOne, String keyTwo, String keyThree, long value) {
        return realTimeStat(keyOne, keyTwo, keyThree, value);
    }

    private final boolean realTimeStat(String key1, String key2, String key3, long value) {
        LogKey logKey = new LogKey(new Object[] { key1, key2, key3 });
        if (!map.containsKey(logKey)) {
            if (map.size() >= maxKeySize) {
                return false; // XXX: 到容量上限后放弃
            }
        }
        long[] values = new long[] { 1L, value };
        // XXX: 目前不进行 key1/key2 的合并统计, 没有这个需求
        // write(new Object[] { key1 }, values);
        // write(new Object[] { key1, key2 }, values);
        write(logKey.getKeys(), values);
        return true;
    }

    private List<SnapshotValuesOutputCallBack> snapshotValueCallBack = new LinkedList<SnapshotValuesOutputCallBack>();

    public synchronized void addSnapshotValuesCallbask(SnapshotValuesOutputCallBack callbackList) {
        if (snapshotValueCallBack.contains(callbackList)) {
            // only one instance is allowed
            return;
        }
        snapshotValueCallBack.add(callbackList);
    }

    public synchronized void removeSnapshotValuesCallback(SnapshotValuesOutputCallBack callbackList) {
        snapshotValueCallBack.remove(callbackList);
    }

    /**
     * 拉取自定义日志内容并打印(单线程，无需锁)
     */
    private final void writeCallBackLog() {
        for (SnapshotValuesOutputCallBack callBack : snapshotValueCallBack) {
            try {
                // XXX: 原来这里会合并同类项, 目前看来无必要
                callBack.snapshotValues(TDDL_Log_Writer);
            } catch (Throwable e) {
                logger.warn("callBack", e);
            }
        }
    }

    /**
     * 将内存数据输出到日志中
     */
    @SuppressWarnings("unused")
    private final StatLogWriter Nagios_Log_Writer = new StatLogWriter() {

                                                      public void write(Object[] keys, Object[] fields, long... values) {
                                                          if (values.length < 2) {
                                                              throw new IllegalArgumentException("At least given 2 values");
                                                          }
                                                          fields = Arrays.copyOf((fields == null) ? keys : fields, 3);
                                                          long count = values[0];
                                                          long value = values[1];
                                                          String averageValueStr = "invalid";
                                                          if (count != 0) {
                                                              double averageValue = (double) value / count;
                                                              averageValueStr = String.valueOf(averageValue);
                                                          }
                                                          // String key = new
                                                          // StringBuilder(entry.getKey()).append("||").toString();
                                                          // String value = new
                                                          // StringBuilder().append(values.value1).append("|").append(values.value2).append("|")
                                                          // .append(values.value2).append((double)
                                                          // values.value1.get()
                                                          // /
                                                          // values.value2.get()).toString();
                                                          // NagiosUtils.addNagiosLog(key,
                                                          // value);
                                                          NagiosUtils.addNagiosLog(TStringUtil.join(fields, "|"), // NL
                                                              count + "|" + value + "|" + averageValueStr);
                                                      }
                                                  };

    /**
     * 将内存数据输出到日志中 SELECT xxx
     * #@#my065037_cm4_feel_25#@#EXECUTE_A_SQL_SUCCESS#@#1
     * #@#1#@#1#@#1#@#10-12-27 13:58:35:224 SELECT sss
     * #@#my065026_cm4_feel_03#@#EXECUTE_A_SQL_SUCCESS
     * #@#1#@#1#@#1#@#1#@#10-12-27 13:58:35:224
     */
    private final StatLogWriter TDDL_Log_Writer   = new LoggerLogWriter(LoggerInit.TDDL_Snapshot_LOG) {

                                                      // XXX: 输出中首先写数据, 然后写信息,
                                                      // 最后写时间
                                                      protected StringBuffer format(StringBuffer buf, Object[] fields,
                                                                                    Date time, long... values) {
                                                          // LoggerInit.TDDL_Snapshot_LOG.warn(new
                                                          // StringBuilder().append(values.value1).append(
                                                          // BufferedStatLogWriter.logFieldSep).append(values.value2).append(
                                                          // BufferedStatLogWriter.logFieldSep).append(key).append(BufferedStatLogWriter.logFieldSep)
                                                          // .append(time).append(BufferedStatLogWriter.linesep));
                                                          for (long value : values) {
                                                              buf.append(value).append(fieldSeperator);
                                                          }
                                                          for (Object field : fields) {
                                                              buf.append(field).append(fieldSeperator);
                                                          }
                                                          return buf.append(df.format(time)).append(lineSeperator);
                                                      }
                                                  };
}
