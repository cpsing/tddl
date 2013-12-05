package com.taobao.tddl.repo.mysql.spi;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class My_Log {

    private static final Logger log = LoggerFactory.getLogger(My_Log.class);

    public static Logger getLog() {
        return log;
    }
}
