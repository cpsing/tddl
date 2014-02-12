package com.taobao.tddl.common.model;

/**
 * 用于执行的ExtraCmd
 * 
 * @author Dreamond
 */
public class ExtraCmd {

    public static final String OPTIMIZER_CACHE                       = "OPTIMIZER_CACHE";

    /**
     * 是否选取索引，默认为true
     */
    public final static String CHOOSE_INDEX                          = "CHOOSE_INDEX";

    /**
     * 是否选择最优join策略, 默认为false
     */
    public final static String CHOOSE_JOIN                           = "CHOOSE_JOIN";

    /**
     * 是否将or条件转化为index merge，默认为false
     */
    public final static String CHOOSE_INDEX_MERGE                    = "CHOOSE_INDEX_MERGE";

    /**
     * 智能优化join merge join，默认为true
     */
    public final static String JOIN_MERGE_JOIN_JUDGE_BY_RULE         = "JOIN_MERGE_JOIN_JUDGE_BY_RULE";

    /**
     * 是否强制优化成join merge join，默认为false
     */
    public final static String JOIN_MERGE_JOIN                       = "JOIN_MERGE_JOIN";

    /**
     * 是否展开Merge Join Merge，默认为false
     */
    public final static String MERGE_EXPAND                          = "MERGE_EXPAND";

    /**
     * 是否设置Merge并行执行，默认为true
     */
    public final static String MERGE_CONCURRENT                      = "MERGE_CONCURRENT";

    /**
     * 表的meta超时时间，单位毫秒，默认5分钟
     */
    public static final String TABLE_META_CACHE_EXPIRE_TIME          = "TABLE_META_CACHE_EXPIRE_TIME";

    /**
     * 优化器和parser结果超时时间，单位毫秒，默认5分钟
     */
    public static final String OPTIMIZER_CACHE_EXPIRE_TIME           = "OPTIMIZER_CACHE_EXPIRE_TIME";

    /**
     * 如果这个值为true,则允许使用临时表。 而如果为空。或者为false,则不允许使用临时表。
     * 从性能和实际需求来说，默认值应该为false.也就是不允许使用临时表。
     */
    public static final String ALLOW_TEMPORARY_TABLE                 = "ALLOW_TEMPORARY_TABLE";

    /**
     * limit数量超过该阀值，启用streaming模式，默认为100
     */
    public static final String STREAMI_THRESHOLD                     = "STREAMI_THRESHOLD";

    /**
     * 创建cursor后是否立马执行
     */
    public static final String EXECUTE_QUERY_WHEN_CREATED            = "EXECUTE_QUERY_WHEN_CREATED";

    public static final String HBASE_MAPPING_FILE                    = "HBASE_MAPPING_FILE";

    /**
     * 执行jdbc fetch size
     */
    public static final String FETCH_SIZE                            = "FETCH_SIZE";

    /**
     * 标记是否关闭Join Order优化
     */
    public static final String INIT_TDDL_DATASOURCE                  = "INIT_TDDL_DATASOURCE";

    /**
     * 是否使用tdhs替换jdbc调用
     */
    public static final String USE_TDHS_FOR_DEFAULT                  = "USE_TDHS_FOR_DEFAULT";

    /**
     * 为每个连接都初始化一个线程池，用来做并行查询，默认为true
     */
    public static final String INIT_CONCURRENT_POOL_EVERY_CONNECTION = "INIT_CONCURRENT_POOL_EVERY_CONNECTION";

    /**
     * 并行查询线程池大小
     */
    public static final String CONCURRENT_THREAD_SIZE                = "CONCURRENT_THREAD_SIZE";

}
