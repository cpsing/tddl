package com.taobao.tddl.monitor;

import com.taobao.tddl.monitor.stat.StatLogWriter;

/**
 * 一些静态值的处理，静态值不符合累加模型，因此在输出的时候回调这个接口加入到old里面输出
 * 
 * @author changyuan.lh
 * @author shenxun
 * @author junyu
 */
public interface SnapshotValuesOutputCallBack {

    public static class Key {

        public static final String replicationQueueSize          = "_replicationQueueSize";
        public static final String replicationPoolSize           = "_replicationPoolSize";
        public static final String parserCacheSize               = "_parserCacheSize";

        public static final String THREAD_COUNT                  = "THREAD_COUNT";
        public static final String THREAD_COUNT_REJECT_COUNT     = "THREAD_COUNT_REJECT_COUNT";
        public static final String READ_WRITE_TIMES              = "READ_WRITE_TIMES";
        public static final String READ_WRITE_TIMES_REJECT_COUNT = "READ_WRITE_TIMES_REJECT_COUNT";
        public static final String READ_WRITE_CONCURRENT         = "READ_WRITE_CONCURRENT";
    }

    /**
     * 当前的统计内容汇总：
     * 
     * @see TDataSourceState
     * @see TDataSourceWrapper
     */
    void snapshotValues(StatLogWriter statLog);
}
