package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.cursor.IColumnAliasCursor;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.rowset.RowSetWrapper;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * 用于做别名替换和select操作
 * 
 * @author mengshi.sunmengshi 2013-12-3 上午10:54:30
 * @since 5.0.0
 */
public class ColumnAliasCursor extends SchematicCursor implements IColumnAliasCursor {

    private final String            tableAlias;
    private final List<ISelectable> retColumns;
    private List<ColumnMeta>        colMessages;
    private boolean                 schemaInited = false;
    private ICursorMeta             newMeta;
    private List<ColumnMeta>        returnColumnMetas;

    public ColumnAliasCursor(ISchematicCursor cursor, List<ISelectable> retColumns, String tableAlias){
        super(cursor);
        this.tableAlias = tableAlias;
        this.retColumns = retColumns;
        // initAliasSchema(retColumns, tableAlias);

    }

    private void initAliasSchema(List<ISelectable> retColumns, String tableAlias, ICursorMeta cursormeta) {
        if (schemaInited) {
            return;
        }
        colMessages = new ArrayList<ColumnMeta>(retColumns.size());
        List<Integer> indexes = new ArrayList<Integer>(colMessages.size());
        for (ISelectable col : retColumns) {
            ColumnMeta cm = null;
            String tableName = col.getTableName();
            if (tableAlias != null) {
                if (tableName == null || tableAlias.startsWith(tableName)) {
                    // 如果以tableName作为开始，那么认为认为是相同名字，因此也做截断处理，取alias的.之前的数据。
                    // 最好也让优化器协助，不允许在alias里面出现"."
                    tableAlias = ExecUtils.getLogicTableName(tableAlias);
                }
            }
            if (!TStringUtil.isBlank(col.getAlias())) {
                if (TStringUtil.isBlank(tableAlias)) {
                    cm = ExecUtils.getColumnMeta(col, col.getAlias());
                } else {
                    cm = ExecUtils.getColumnMeta(col, tableAlias, col.getAlias());
                }
            } else {
                if (TStringUtil.isBlank(tableAlias)) {
                    cm = ExecUtils.getColumnMeta(col);
                } else {
                    cm = ExecUtils.getColumnMetaTable(col, tableAlias);
                }
            }
            Integer index = cursormeta.getIndex(col.getTableName(), col.getColumnName());
            if (index == null) {
                index = cursormeta.getIndex(col.getTableName(), col.getAlias());
            }

            if (index != null) {
                indexes.add(index);
                colMessages.add(cm);
            }
        }
        if (!TStringUtil.isBlank(tableAlias)) {
            // 如果没有就用下层的tableName
            newMeta = CursorMetaImp.buildNew(colMessages, indexes, cursormeta.getIndexRange());
            List<IOrderBy> obOld = getOrderBy();
            if (obOld != null) {
                List<IOrderBy> obNew = new ArrayList<IOrderBy>(obOld.size());
                for (IOrderBy orderBy : obOld) {
                    IColumn icol = ExecUtils.getIColumn(orderBy.getColumn());
                    IColumn icolNew = icol.copy();
                    obNew.add(ASTNodeFactory.getInstance()
                        .createOrderBy()
                        .setColumn(icolNew.setTableName(tableAlias).setAlias(null))
                        .setDirection(orderBy.getDirection()));
                }
                setOrderBy(obNew);
            }
        } else {
            newMeta = CursorMetaImp.buildNew(colMessages, indexes, cursormeta.getIndexRange());
        }
        schemaInited = true;
    }

    @Override
    public IRowSet next() throws TddlException {
        // 不做判断是否需要replace和select，只进行浅封装
        return replaceAliasAndSelect(parentCursorNext());
    }

    @Override
    public IRowSet prev() throws TddlException {
        return replaceAliasAndSelect(parentCursorPrev());
    }

    @Override
    public IRowSet current() throws TddlException {
        return replaceAliasAndSelect(parentCursorCurrent());
    }

    @Override
    public IRowSet first() throws TddlException {
        return replaceAliasAndSelect(parentCursorFirst());
    }

    @Override
    public IRowSet getNextDup() throws TddlException {
        return replaceAliasAndSelect(parentCursorGetNextDup());
    }

    @Override
    public IRowSet last() throws TddlException {
        return replaceAliasAndSelect(parentCursorLast());
    }

    private IRowSet replaceAliasAndSelect(IRowSet ir) {
        if (ir == null) {
            return null;
        }
        initAliasSchema(retColumns, tableAlias, ir.getParentCursorMeta());
        IRowSet ret = new RowSetWrapper(newMeta, ir);
        return ret;
    }

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) throws TddlException {
        Map<CloneableRecord, DuplicateKVPair> res = super.mgetWithDuplicate(keys, prefixMatch, keyFilterOrValueFilter);
        for (DuplicateKVPair dkv : res.values()) {
            while (dkv != null) {
                dkv.currentKey = replaceAliasAndSelect(dkv.currentKey);
                dkv = dkv.next;
            }
        }
        return res;
    }

    @Override
    public String toStringWithInden(int inden) {
        String tabTittle = GeneralUtil.getTab(inden);
        String tabContent = GeneralUtil.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();
        GeneralUtil.printlnToStringBuilder(sb, tabTittle + "ColumnAliasCursor ");
        GeneralUtil.printAFieldToStringBuilder(sb, "tableAlias", this.tableAlias, tabContent);
        GeneralUtil.printAFieldToStringBuilder(sb, "colMessages", this.colMessages, tabContent);
        GeneralUtil.printAFieldToStringBuilder(sb, "retColumns", this.retColumns, tabContent);
        GeneralUtil.printAFieldToStringBuilder(sb, "newMeta", this.newMeta, tabContent);
        if (this.cursor != null) {
            sb.append(this.cursor.toStringWithInden(inden + 1));
        }

        return sb.toString();
    }

    @Override
    public List<ColumnMeta> getReturnColumns() throws TddlException {
        if (this.returnColumnMetas != null) {
            return this.returnColumnMetas;
        }
        returnColumnMetas = new ArrayList();
        for (ISelectable<ISelectable> cm : retColumns) {
            String tableName = tableAlias;
            if (tableName == null) {
                tableName = cm.getTableName();
            }
            returnColumnMetas.add(new ColumnMeta(tableName, cm.getColumnName(), cm.getDataType(), cm.getAlias(), true));
        }
        return returnColumnMetas;
    }
}
