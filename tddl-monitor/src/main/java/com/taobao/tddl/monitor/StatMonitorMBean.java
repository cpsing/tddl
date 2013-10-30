package com.taobao.tddl.monitor;

/**
 * @author guangxia
 * @since 1.0, 2010-2-9 下午03:40:20
 */
public interface StatMonitorMBean {

    /**
     * 重新开始实时统计
     */
    void resetStat();

    /**
     * 最新统计的时间点
     * 
     * @return
     */
    long getStatDuration();

    /**
     * 获取实时统计结果
     * 
     * @param key1
     * @param key2
     * @param key3
     * @return
     */
    String getStatResult(String key1, String key2, String key3);

    long getDuration();

}
