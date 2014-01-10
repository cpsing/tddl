package com.taobao.tddl.common.model;

/**
 * 用于执行的ExtraCmd
 * 
 * @author Dreamond
 */
public class ExtraCmd {

    public static class OptimizerExtraCmd {

        /**
         * 为true时，选取索引，
         */
        public final static String ChooseIndex              = "ChooseIndex";

        /**
         * 为true时，最优join策略选择
         */
        public final static String ChooseJoin               = "ChooseJoin";

        /**
         * 为true时，会将or条件转化为index merge
         */
        public final static String ChooseIndexMerge         = "ChooseIndexMerge";

        /**
         * 智能优化join merge join
         */
        public final static String JoinMergeJoinJudgeByRule = "JoinMergeJoinJudgeByRule";

        /**
         * 强制优化成join merge join
         */
        public final static String JoinMergeJoin            = "JoinMergeJoin";

        /**
         * 为true时，Merge Join Merge将会展开
         */
        public final static String MergeExpand              = "MergeExpand";

        /**
         * 为true时，Merge将并行执行
         */
        public final static String MergeConcurrent          = "MergeConcurrent";
    }

    public static class ExecutionExtraCmd {

        /**
         * 似乎是没用了
         */
        public static final String INDEX_NAME                 = "INDEX_NAME";
        /**
         * 用于记录这次请求从一个dbd 的 dbGroup里面选择了哪一个bdb进行查询。
         */
        public static final String GROUP_INDEX                = "GROUP_INDEX";
        /**
         * 用于决定当前bdb 数据查询是否是一个强一致查询
         */
        public static final String GROUP_CONSISTENT           = "GROUP_CONSISTENT";
        /**
         * 用于记录这次请求所走的具体是哪一个dbGroup.
         */
        public static final String MATRIX_KEY                 = "MATRIX_KEY";

        /**
         * 如果这个值为true,则允许使用临时表。 而如果为空。或者为false,则不允许使用临时表。
         * 从性能和实际需求来说，默认值应该为false.也就是不允许使用临时表。
         */
        public static final String ALLOW_TEMPORARY_TABLE      = "ALLOW_TEMPORARY_TABLE";

        /**
         * 查询是否是流模式
         */
        public static final String STREAM_MODE                = "STREAM_MODE";
        public static final String EXECUTE_QUERY_WHEN_CREATED = "EXECUTE_QUERY_WHEN_CREATED";

        /**
         * 是否允许某些查询使用BIO，默认为true
         */
        public static final String ALLOW_BIO                  = "ALLOW_BIO";

        public static final String HBASE_MAPPING_FILE         = "HBASE_MAPPING_FILE";
        public static final String FETCH_SIZE                 = "FETCH_SIZE";
    }

    public static class ConnectionExtraCmd {

        /**
         * 标记是否关闭Join Order优化
         */
        public static final String OPTIMIZE_JOIN_ORDER                    = "OPTIMIZE_JOIN_ORDER";
        public static final String INIT_TEMP_TABLE                        = "USE_TEMP_TABLE";
        public static final String INIT_TDDL_DATASOURCE                   = "INIT_TDDL_DATASOURCE";

        /**
         * 表的meta超时时间，单位毫秒
         */
        public static final String TABLE_META_CACHE_EXPIRE_TIME           = "TABLE_META_CACHE_EXPIRE_TIME";

        /**
         * 优化器和parser结果超时时间，单位毫秒
         */
        public static final String OPTIMIZER_CACHE_EXPIRE_TIME            = "OPTIMIZER_CACHE_EXPIRE_TIME";

        public static final String CONFIG_UPDATE_INVALID_MINUTE           = "CONFIG_UPDATE_INVALID_MINUTE";
        public static final String USE_TDHS_FOR_DEFAULT                   = "USE_TDHS_FOR_DEFAULT";
        public static final String USE_BOTH_LOCALSCHEMA_AND_DYNAMICSCHEMA = "USE_BOTH_LOCALSCHEMA_AND_DYNAMICSCHEMA";
        public static final String RULE                                   = "RULE";
        public static final String CONFIG_DATA_HANDLER_FACTORY            = "CONFIG_DATA_HANDLER_FACTORY";
        public static final String PARSER_CACHE                           = "PARSER_CACHE";
        public static final String OPTIMIZER_CACHE                        = "OPTIMIZER_CACHE";

        /**
         * 为每个连接都初始化一个线程池，用来做并行查询，默认为true
         */
        public static final String INIT_CONCURRENT_POOL_EVERY_CONNECTION  = "INIT_CONCURRENT_POOL_EVERY_CONNECTION";

        /**
         * 并行查询线程池大小
         */
        public static final String CONCURRENT_THREAD_SIZE                 = "CONCURRENT_THREAD_SIZE";
    }

}
