package com.taobao.tddl.executor.cursor;

import java.util.Iterator;
import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.ExceptionErrorCodeUtils;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.rowset.IRowSet;

/**
 * @author mengshi.sunmengshi 2013-11-29 下午1:38:39
 * @since 5.0.0
 */
public class ResultCursor extends SchematicCursor {

    public static EmptyResultCursor EMPTY_RESULT_CURSOR = new EmptyResultCursor(null, null);

    private static class EmptyResultCursor extends ResultCursor {

        public EmptyResultCursor(ISchematicCursor cursor, ExecutionContext executionContext){
            super(cursor, executionContext);
        }

        @Override
        public List<Object> getOriginalSelectColumns() {
            return null;
        }

        @Override
        public void setOriginalSelectColumns(List<Object> originalSelectColumns) {

        }

        @Override
        public String getException() {
            return null;
        }

        @Override
        public Integer getResultID() {
            return null;
        }

        @Override
        public void setSize(int n) {

        }

        @Override
        public int getTotalCount() {
            return 0;
        }

        @Override
        public Long getTxnID() {
            return null;
        }

        @Override
        protected void closeStatus() {

        }

        @Override
        protected void throwExceptionIfClosed() {

        }

        @Override
        public ResultCursor setTransactionID(Long txn_id) {
            return this;
        }

        @Override
        public ResultCursor setResultID(Integer result_id) {
            return this;
        }

        @Override
        public ResultCursor setException(String exception) {
            return this;
        }

        @Override
        public ResultCursor setResultCount(Integer count) {
            return this;
        }

        @Override
        public ResultCursor setResults(List<IRowSet> results) {
            return this;
        }

        @Override
        public Object getIngoreTableName(IRowSet kv, String column) {
            return null;
        }

        @Override
        public Object get(IRowSet kv, String table, String column) {
            return null;
        }

        @Override
        public IRowSet next() throws TddlException {
            return null;
        }

        @Override
        public String getException(Exception e, ResultCursor cursor) {
            return null;
        }

        @Override
        public void beforeFirst() throws TddlException {

        }

        @Override
        public String toStringWithInden(int inden) {
            return "This is a empty ResultCursor";
        }

        @Override
        public String toString() {
            return "This is a empty ResultCursor";
        }

    }

    // private static final String INDEX_NAME = "INDEX_NAME";

    public static final String  AFFECT_ROW = "AFFECT_ROW";
    protected boolean           closed     = false;
    protected Long              txn_id;
    protected Integer           result_id;
    protected String            exception;
    protected List<IRowSet>     results;
    protected Iterator<IRowSet> iter;
    protected int               size       = 10;
    protected int               totalCount;
    protected List<Object>      originalSelectColumns;
    private ExecutionContext    executionContext;

    public List<Object> getOriginalSelectColumns() {
        return originalSelectColumns;
    }

    public void setOriginalSelectColumns(List<Object> originalSelectColumns) {
        this.originalSelectColumns = originalSelectColumns;
    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    public void setExecutionContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    //
    // public List<IRowSet> getResults() {
    // return results;
    // }

    public String getException() {
        return exception;
    }

    public Integer getResultID() {
        return result_id;
    }

    public void setSize(int n) {
        this.size = n;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public Long getTxnID() {
        return txn_id;
    }

    protected void closeStatus() {
        closed = true;
    }

    protected void throwExceptionIfClosed() {
        if (closed) {
            throw new IllegalStateException("alread closed");
        }
    }

    public ResultCursor(ISchematicCursor cursor, ExecutionContext executionContext){
        super(cursor, null, cursor == null ? null : cursor.getOrderBy());
        this.executionContext = executionContext;
    }

    public ResultCursor(ISchematicCursor cursor, ExecutionContext executionContext, List<Object> originalSelectColumns){
        super(cursor, null, cursor == null ? null : cursor.getOrderBy());
        this.executionContext = executionContext;
        this.originalSelectColumns = originalSelectColumns;
    }

    public ResultCursor setTransactionID(Long txn_id) {
        this.txn_id = txn_id;
        return this;
    }

    public ResultCursor setResultID(Integer result_id) {
        this.result_id = result_id;
        return this;
    }

    public ResultCursor setException(String exception) {
        this.exception = exception;
        return this;
    }

    public ResultCursor setResultCount(Integer count) {
        this.totalCount = count;
        return this;
    }

    public ResultCursor setResults(List<IRowSet> results) {
        this.results = results;
        if (results != null) {
            iter = results.iterator();
        }
        return this;
    }

    public Object getIngoreTableName(IRowSet kv, String column) {
        if (kv == null) {
            return null;
        }
        Integer index = getIndex(kv, column, null);
        return kv.getObject(index);
    }

    private Integer getIndex(IRowSet kv, String column, String tableName) {
        Integer index = kv.getParentCursorMeta().getIndex(tableName, column);
        if (index == null) {
            throw new IllegalArgumentException("can't find index by " + tableName + "." + column + " .");
        }
        return index;
    }

    public Object get(IRowSet kv, String table, String column) {
        if (kv == null) {
            return null;
        }
        Integer index = getIndex(kv, column, table);
        return kv.getObject(index);
    }

    @Override
    public IRowSet next() throws TddlException {
        if (closed) {
            return null;
        }
        return parentCursorNext();
    }

    public String getException(Exception e, ResultCursor cursor) {
        if (e instanceof TddlException) {
            // 已知异常，UstoreException是有状态码的，定义过处理逻辑。
            return e.getMessage();
        }
        String targetException = "";
        if (cursor != null) {
            targetException = cursor.getException();
        }
        // log.warn(ExceptionErrorCodeUtils.UNKNOWN_EXCEPTION + "", e);
        String exception = ExceptionErrorCodeUtils.appendErrorCode(ExceptionErrorCodeUtils.UNKNOWN_EXCEPTION, e)
                           + "\n------\n acturalException : " + targetException;
        return exception;
    }

    @Override
    public void beforeFirst() throws TddlException {
        throwExceptionIfClosed();
        GeneralUtil.checkInterrupted();
        if (result_id == null) {
            if (this.results == null) return;

            iter = results.iterator();

        } else {
            if (results != null) {
                results.clear();
                iter = results.iterator();
            }
            super.beforeFirst();
        }
    }

    @Override
    public List<TddlException> close(List<TddlException> exceptions) {
        if (closed) {
            return exceptions;
        }
        closed = true;
        List<TddlException> ex = parentCursorClose(exceptions);
        return ex;
    }

    @Override
    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        sb.append("result : ").append("\n");
        sb.append(cursor.toStringWithInden(inden));
        return sb.toString();
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }
}
