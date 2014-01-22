package com.taobao.tddl.executor.cursor.impl;

import java.util.Arrays;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.cursor.IAffectRowCursor;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.rowset.ArrayRowSet;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMessage;
import com.taobao.tddl.optimizer.core.datatype.DataType;

public class AffectRowCursor extends SchematicCursor implements IAffectRowCursor {

    private int           affectRow    = 0;
    protected boolean     first        = true;
    private boolean       schemaInited = false;
    protected ICursorMeta cursormeta;

    public AffectRowCursor(int affectRow){
        super(null, null, null);
        this.affectRow = affectRow;
    }

    protected ICursorMeta initSchema() {
        if (schemaInited) {
            return cursormeta;
        }
        schemaInited = true;
        ColumnMessage colMeta = new ColumnMessage(ResultCursor.AFFECT_ROW, DataType.IntegerType);
        CursorMetaImp cursurMetaImp = CursorMetaImp.buildNew("", Arrays.asList(colMeta), 1);
        this.cursormeta = cursurMetaImp;
        return cursurMetaImp;
    }

    @Override
    public IRowSet next() throws TddlException {
        initSchema();
        if (!first) {
            return null;
        }
        first = false;
        ArrayRowSet arrayRowSet = new ArrayRowSet(1, cursormeta);
        arrayRowSet.setInteger(0, affectRow);
        return arrayRowSet;
    }

    @Override
    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String tab = GeneralUtil.getTab(inden);
        sb.append(tab).append("【AffectRowCursor : ").append("\n");
        ExecUtils.printMeta(cursormeta, inden, sb);
        ExecUtils.printOrderBy(orderBys, inden, sb);
        sb.append(super.toStringWithInden(inden + 1));
        return sb.toString();
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }
}
