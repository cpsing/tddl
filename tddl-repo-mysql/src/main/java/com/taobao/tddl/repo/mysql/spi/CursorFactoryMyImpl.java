package com.taobao.tddl.repo.mysql.spi;

import java.util.List;

import com.taobao.tddl.executor.common.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.CursorFactoryDefaultImpl;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.repo.mysql.cursor.SchematicMyCursor;

public class CursorFactoryMyImpl extends CursorFactoryDefaultImpl {

    public CursorFactoryMyImpl(AndorContext clientContext){
        super(clientContext);
    }

    @Override
    public ISchematicCursor schematicCursor(Cursor cursor, ICursorMeta meta, List<IOrderBy> orderBys) {
        try {
            return new SchematicMyCursor(cursor, meta, orderBys);
        } catch (RuntimeException e) {
            closeParentCursor(cursor);
            throw e;
        }
    }

}
