package com.taobao.tddl.repo.mysql.spi;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.cursor.Cursor;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.record.FixedLengthRecord;
import com.taobao.tddl.executor.record.NamedRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.parse.TableMetaParser;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.utils.FilterUtils;

/**
 * @author mengshi.sunmengshi 2013-12-6 下午6:13:21
 * @since 5.1.0
 */
public class My_Cursor implements Cursor {

    protected My_JdbcHandler    myJdbcHandler;
    protected IDataNodeExecutor query;
    protected ICursorMeta       meta;
    protected boolean           inited        = false;
    // private boolean directlyExecuteSql = false;

    protected boolean           isStreaming   = false;
    protected List<ColumnMeta>  returnColumns = null;

    public My_Cursor(My_JdbcHandler myJdbcHandler, ICursorMeta meta, IDataNodeExecutor executor, boolean isStreaming){
        super();
        this.myJdbcHandler = myJdbcHandler;
        this.query = executor;
        this.meta = meta;
        this.isStreaming = isStreaming;
    }

    @Override
    public boolean skipTo(CloneableRecord key) throws TddlException {
        init();
        return true;
    }

    @Override
    public boolean skipTo(KVPair key) throws TddlException {
        throw new UnsupportedOperationException("not support yet");
    }

    @Override
    public IRowSet current() throws TddlException {
        init();
        return myJdbcHandler.getCurrent();
    }

    @Override
    public IRowSet next() throws TddlException {
        init();
        try {
            return myJdbcHandler.next();
        } catch (SQLException e) {
            throw new TddlException(e);
        }
    }

    public void init() throws TddlException {
        if (inited) {
            return;
        }
        try {
            myJdbcHandler.executeQuery(meta, isStreaming);
            ResultSetMetaData rsmd = this.myJdbcHandler.getResultSet().getMetaData();
            returnColumns = new ArrayList();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                DATA_TYPE type = TableMetaParser.jdbcTypeToDataType(rsmd.getColumnType(i));
                if (type == null) throw new IllegalArgumentException("列：" + rsmd.getColumnName(i) + " 类型"
                                                                     + rsmd.getColumnType(i) + "无法识别,联系沈询");

                String name = rsmd.getColumnLabel(i);

                ColumnMeta cm = new ColumnMeta(null, name, type, null, true);
                returnColumns.add(cm);
            }
            inited = true;
        } catch (SQLException e) {
            throw new TddlException(e);
        }
    }

    public ISchematicCursor getResultSet() throws TddlException {
        init();

        return myJdbcHandler.getResultCursor();
    }

    @Override
    public IRowSet prev() throws TddlException {
        isStreaming = false;
        init();
        try {
            return myJdbcHandler.prev();
        } catch (SQLException e) {
            throw new TddlException(e);
        }
    }

    @Override
    public IRowSet first() throws TddlException {
        init();
        try {
            return myJdbcHandler.first();
        } catch (SQLException e) {
            throw new TddlException(e);
        }
    }

    @Override
    public IRowSet last() throws TddlException {
        init();
        try {
            return myJdbcHandler.last();
        } catch (SQLException e) {
            throw new TddlException(e);
        }
    }

    @Override
    public boolean delete() throws TddlException {
        throw new UnsupportedOperationException("not support yet");
    }

    @Override
    public IRowSet getNextDup() throws TddlException {
        throw new UnsupportedOperationException("not support yet");
    }

    @Override
    public void put(CloneableRecord key, CloneableRecord value) throws TddlException {
        throw new UnsupportedOperationException("not support yet");
    }

    public ICursorMeta getCursorMeta() {
        return meta;
    }

    public void setCursorMeta(ICursorMeta cursorMeta) {
        this.meta = cursorMeta;
    }

    public IDataNodeExecutor getiQuery() {
        return query;
    }

    public void setiQuery(IQuery iQuery) {
        this.query = iQuery;
    }

    @Override
    public List<TddlException> close(List<TddlException> exs) {
        if (exs == null) {
            exs = new ArrayList();
        }
        try {
            myJdbcHandler.close();
        } catch (Exception e) {
            exs.add(new TddlException(e));
        }

        return exs;
    }

    public int sizeLimination = 10000;

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) throws TddlException {

        IQuery tmpQuery = (IQuery) query.copy();

        List<Comparable> values = new ArrayList<Comparable>();
        String cm = keys.get(0).getColumnList().get(0);
        for (CloneableRecord key : keys) {
            values.add((Comparable) key.get(cm));
        }
        IColumn ic = ASTNodeFactory.getInstance().createColumn();
        ic.setColumnName(cm);

        IBooleanFilter targetFilter = ASTNodeFactory.getInstance().createBooleanFilter();
        targetFilter.setOperation(OPERATION.IN);
        targetFilter.setColumn(ic);
        targetFilter.setValues(values);

        tmpQuery.setKeyFilter(FilterUtils.and(tmpQuery.getKeyFilter(), targetFilter));

        myJdbcHandler.setPlan(tmpQuery);
        try {
            myJdbcHandler.executeQuery(this.meta, isStreaming);
        } catch (SQLException e) {
            throw new TddlException(e);
        }
        Map<CloneableRecord, DuplicateKVPair> res = buildDuplicateKVPair(keys);
        return res;
    }

    // ==============Getters and Setters=======

    public Map<CloneableRecord, DuplicateKVPair> buildDuplicateKVPair(List<CloneableRecord> keys) throws TddlException {
        String cmStr = keys.get(0).getColumnList().get(0);
        ColumnMeta cm = new ColumnMeta(getCursorMeta().getColumns().get(0).getTableName(), cmStr, null, null, true);
        List<ColumnMeta> cms = new LinkedList<ColumnMeta>();
        cms.add(cm);
        IRowSet rowSet = null;
        int count = 0;
        Map<CloneableRecord, DuplicateKVPair> duplicateKeyMap = new HashMap<CloneableRecord, DuplicateKVPair>();
        try {
            while ((rowSet = myJdbcHandler.next()) != null) {
                CloneableRecord value = new FixedLengthRecord(cms);
                CloneableRecord key = new NamedRecord(cmStr, value);
                rowSet = ExecUtils.fromIRowSetToArrayRowSet(rowSet);
                Object v = ExecUtils.getValueByColumnMeta(rowSet, cm);
                value.put(cmStr, v);
                DuplicateKVPair tempKVPair = duplicateKeyMap.get(key);
                if (tempKVPair == null) {// 加新列
                    tempKVPair = new DuplicateKVPair(rowSet);
                    duplicateKeyMap.put(key, tempKVPair);
                } else {// 加重复列

                    while (tempKVPair.next != null) {
                        tempKVPair = tempKVPair.next;
                    }

                    tempKVPair.next = new DuplicateKVPair(rowSet);
                }
                count++;
                if (count >= sizeLimination) {// 保护。。。别太多了
                    throw new IllegalArgumentException("size is more than limination " + sizeLimination);
                }
            }
        } catch (SQLException e) {
            throw new TddlException(e);
        }
        if (rowSet == null) {
            try {
                myJdbcHandler.close();
            } catch (SQLException e) {
                throw new TddlException(e);
            }
        }
        return duplicateKeyMap;
    }

    @Override
    public List<DuplicateKVPair> mgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                       boolean keyFilterOrValueFilter) throws TddlException {
        Map<CloneableRecord, DuplicateKVPair> map = mgetWithDuplicate(keys, prefixMatch, keyFilterOrValueFilter);
        return new ArrayList<DuplicateKVPair>(map.values());
    }

    @Override
    public String toStringWithInden(int inden) {

        String tabTittle = GeneralUtil.getTab(inden);
        String tabContent = GeneralUtil.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();

        GeneralUtil.printlnToStringBuilder(sb, tabTittle + "MyCursor ");
        if (meta != null) {
            GeneralUtil.printAFieldToStringBuilder(sb, "meta", this.meta, tabContent);
        }

        GeneralUtil.printAFieldToStringBuilder(sb, "isStreaming", this.isStreaming, tabContent);

        if (this.myJdbcHandler != null) GeneralUtil.printAFieldToStringBuilder(sb,
            "plan",
            this.myJdbcHandler.getPlan(),
            tabContent);

        return sb.toString();
    }

    @Override
    public void beforeFirst() throws TddlException {
        init();
        try {
            myJdbcHandler.beforeFirst();
        } catch (SQLException e) {
            throw new TddlException(e);
        }
    }

    @Override
    public List<ColumnMeta> getReturnColumns() throws TddlException {
        init();
        return this.returnColumns;
    }

    @Override
    public boolean isDone() {
        return true;
    }

}
