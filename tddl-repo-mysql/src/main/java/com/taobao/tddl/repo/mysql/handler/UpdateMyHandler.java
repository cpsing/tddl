package com.taobao.tddl.repo.mysql.handler;

import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;

/**
 * @author mengshi.sunmengshi 2013-12-5 下午6:28:28
 * @since 5.0.0
 */
public class UpdateMyHandler extends PutMyHandlerCommon {

    private static final Logger log = LoggerFactory.getLogger(UpdateMyHandler.class);

    public UpdateMyHandler(){
        super();
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected ISchematicCursor executePut(ExecutionContext executionContext, IPut put, ITable table, IndexMeta meta,
                                          My_JdbcHandler myJdbcHandler) throws TddlException {
        if (put.getQueryTree() == null) {
            log.warn("注意，做了全表更新操作");
        }
        try {
            myJdbcHandler.executeUpdate(executionContext, put, table, meta);
        } catch (SQLException e) {
            throw new TddlException(e);
        }
        return myJdbcHandler.getResultCursor();
    }
}
