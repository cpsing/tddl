package com.taobao.tddl.monitor.logger;

import com.taobao.tddl.monitor.logger.log4j.DynamicLog4jLogger;
import com.taobao.tddl.monitor.logger.logback.DynamicLogbackLogger;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class LoggerInit {

    public static final String TDDL_ATOM_STATISTIC_LOG_NAME = "TDDL_Atom_Statistic_LOG";
    public static final Logger TDDL_LOG                     = LoggerFactory.getLogger("TDDL_LOG");
    public static final Logger TDDL_SQL_LOG                 = LoggerFactory.getLogger("TDDL_SQL_LOG");
    public static final Logger TDDL_MD5_TO_SQL_MAPPING      = LoggerFactory.getLogger("TDDL_MD5_TO_SQL_MAPPING");
    public static final Logger TDDL_Nagios_LOG              = LoggerFactory.getLogger("TDDL_Nagios_LOG");
    // modify by junyu ,atom 和matrix拆开
    public static final Logger TDDL_Atom_Statistic_LOG      = LoggerFactory.getLogger(TDDL_ATOM_STATISTIC_LOG_NAME);
    public static final Logger TDDL_Matrix_Statistic_LOG    = LoggerFactory.getLogger("TDDL_Matrix_Statistic_LOG");
    // add by changyuan.lh, db 应用连接数, 阻塞时间, 超时数
    public static final Logger TDDL_Conn_Statistic_LOG      = LoggerFactory.getLogger("TDDL_Conn_Statistic_LOG");

    public static final Logger TDDL_Statistic_LOG           = LoggerFactory.getLogger("TDDL_Statistic_LOG");
    public static final Logger TDDL_Snapshot_LOG            = LoggerFactory.getLogger("TDDL_Snapshot_LOG");
    public static final Logger logger                       = TDDL_LOG;                                             // Logger.getLogger(LoggerInit.class);

    // tddl rule相关日志，需要独立出来，rule会被多个地方共享
    public static final Logger DB_TAB_LOG                   = LoggerFactory.getLogger("DB_TAB_LOG");
    public static final Logger VSLOT_LOG                    = LoggerFactory.getLogger("VSLOT_LOG");
    public static final Logger DYNAMIC_RULE_LOG             = LoggerFactory.getLogger("DYNAMIC_RULE_LOG");

    static {
        initTddlLog();
    }

    static public void initTddlLog() {
        DynamicLogger dynamic = buildDynamic();

        if (dynamic != null) {
            dynamic.init();
        }
    }

    static public void initRuleLog() {
        DynamicLogger dynamic = buildDynamic();

        if (dynamic != null) {
            dynamic.initRule();
        }
    }

    private synchronized static DynamicLogger buildDynamic() {
        DynamicLogger dynamic = null;
        String LOGBACK = "logback";
        String LOG4J = "log4j";

        // slf4j只是一个代理工程，需要判断一下具体的实现类
        if (checkLogger(logger, LOGBACK)) {
            dynamic = new DynamicLogbackLogger();
        } else if (checkLogger(logger, LOG4J)) {
            dynamic = new DynamicLog4jLogger();
        } else {
            logger.warn("logger is not a log4j/logback instance, dynamic logger is disabled");
        }
        return dynamic;
    }

    private static boolean checkLogger(Logger logger, String name) {
        return logger.getDelegate().getClass().getName().contains(name);
    }
}
