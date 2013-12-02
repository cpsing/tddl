package com.taobao.tddl.executor.cursor;

import java.util.List;

import com.taobao.tddl.optimizer.core.expression.IOrderBy;

public interface ISchematicCursor extends Cursor {

    // public ICursorMeta getMeta() ;
    public List<IOrderBy> getOrderBy();

}
