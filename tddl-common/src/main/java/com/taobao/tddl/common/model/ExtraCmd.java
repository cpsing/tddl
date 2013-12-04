package com.taobao.tddl.common.model;

/**
 * 用于执行的ExtraCmd
 * 
 * @author Dreamond
 */
public class ExtraCmd {

    public static class OptimizerExtraCmd {

        /**
         * 为true时，Merge将并行执行
         */
        public final static String MergeConcurrent          = "MergeConcurrent";

        /**
         * 为true/null时，选取索引，
         */
        public final static String ChooseIndex              = "ChooseIndex";

        /**
         * 此选项为True时将关闭分库分表
         */
        public final static String OffSharding              = "OffSharding";

        public final static String ExpandLeft               = "ExpandLeft";

        public final static String ExpandRight              = "ExpandRight";

        /**
         * 强制优化成join merge join
         */
        public final static String JoinMergeJoin            = "JOIN_MERGE_JOIN";

        /**
         * 智能优化join merge join
         */
        public final static String JoinMergeJoinJudgeByRule = "JoinMergeJoinJudgeByRule";
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
        public static final String CLEAN_SCHEMA_MINUTE                    = "CLEAN_SCHEMA_MINUTE";
        public static final String CONFIG_UPDATE_INVALID_MINUTE           = "CONFIG_UPDATE_INVALID_MINUTE";
        public static final String USE_TDHS_FOR_DEFAULT                   = "USE_TDHS_FOR_DEFAULT";
        public static final String USE_BOTH_LOCALSCHEMA_AND_DYNAMICSCHEMA = "USE_BOTH_LOCALSCHEMA_AND_DYNAMICSCHEMA";
        public static final String RULE                                   = "RULE";
        public static final String CONFIG_DATA_HANDLER_FACTORY            = "CONFIG_DATA_HANDLER_FACTORY";
        public static final String PARSER_CACHE                           = "PARSER_CACHE";
        public static final String OPTIMIZER_CACHE                        = "OPTIMIZER_CACHE";
    }

}
