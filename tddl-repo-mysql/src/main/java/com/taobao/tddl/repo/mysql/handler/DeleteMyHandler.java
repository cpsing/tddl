package com.taobao.tddl.repo.mysql.handler;

import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;

/**
 * @author mengshi.sunmengshi 2013-12-6 上午11:25:53
 * @since 5.0.0
 */
public class DeleteMyHandler extends PutMyHandlerCommon {

    public DeleteMyHandler(){
        super();
    }

    @Override
    protected ISchematicCursor executePut(ExecutionContext executionContext, IPut put, ITable table, IndexMeta meta,
                                          My_JdbcHandler myJdbcHandler) throws TddlException {
        try {
            myJdbcHandler.executeUpdate(executionContext, put, table, meta);
        } catch (SQLException e) {
            throw new TddlException(e);
        }
        return myJdbcHandler.getResultCursor();
    }
}
