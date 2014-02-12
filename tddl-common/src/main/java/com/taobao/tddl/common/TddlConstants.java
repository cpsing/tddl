package com.taobao.tddl.common;

/**
 * @description TDDL整个工程的常量类
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-5-23上午09:54:49
 */
public class TddlConstants {

    /**
     * 初次获取diamond配置超时时间
     */
    public static final long DIAMOND_GET_DATA_TIMEOUT       = 10 * 1000;

    /**
     * default cache expire time, 30000ms
     */
    public static final long DEFAULT_TABLE_META_EXPIRE_TIME = 300 * 1000;

    public static final int  DEFAULT_OPTIMIZER_EXPIRE_TIME  = 300 * 1000;

    public static final int  DEFAULT_STREAM_THRESOLD        = 100;

    public static final int  DEFAULT_CONCURRENT_THREAD_SIZE = 8;
}
