package com.taobao.tddl.repo.mysql.handler;

import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.ExecutionContext;
import com.taobao.tddl.executor.spi.Table;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;

public class ReplaceMyHandler extends PutMyHandlerCommon {

    public ReplaceMyHandler(){
        super();
    }

    @Override
    protected ISchematicCursor executePut(ExecutionContext executionContext, IPut put, Table table, IndexMeta meta,
                                          My_JdbcHandler myJdbcHandler) throws TddlException {
        try {
            myJdbcHandler.executeUpdate(executionContext, put, table, meta);
        } catch (SQLException e) {
            throw new TddlException(e);
        }
        return myJdbcHandler.getResultCursor();
    }
}
