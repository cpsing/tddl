package com.taobao.tddl.repo.mysql.spi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.ParameterMethod;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.AffectRowCursor;
import com.taobao.tddl.executor.cursor.impl.ResultSetCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.repo.mysql.common.ResultSetAutoCloseConnection;
import com.taobao.tddl.repo.mysql.common.ResultSetRemeberIfClosed;
import com.taobao.tddl.repo.mysql.sqlconvertor.MysqlPlanVisitorImpl;
import com.taobao.tddl.repo.mysql.sqlconvertor.SqlAndParam;

/**
 * jdbc 方法执行相关的数据封装. 每个需要执行的cursor都可以持有这个对象进行数据库操作。 ps .. fuxk java
 * ，没有多继承。。只能用组合。。你懂的。。 类不是线程安全的哦亲
 * 
 * @author whisper
 */
public class My_JdbcHandler implements GeneralQueryHandler {

    private static final Logger       log              = LoggerFactory.getLogger(My_JdbcHandler.class);
    protected My_Transaction          myTransaction    = null;
    protected ResultSet               resultSet        = null;
    protected PreparedStatement       ps               = null;
    protected ExecutionType           executionType    = null;
    protected IRowSet                 current          = null;
    protected IRowSet                 prev_kv          = null;
    @SuppressWarnings("unchecked")
    protected Map<String, Comparable> extraCmd         = Collections.EMPTY_MAP;
    protected final static Log        logger           = LogFactory.getLog(My_JdbcHandler.class);
    protected ICursorMeta             cursorMeta;
    protected boolean                 isStreaming      = false;
    protected String                  groupName        = null;
    protected DataSource              ds               = null;
    protected boolean                 strongConsistent = false;
    private ExecutionContext          executionContext = null;

    public enum ExecutionType {
        PUT, GET
    }

    public My_JdbcHandler(ExecutionContext executionContext){
        this.executionContext = executionContext;
    }

    public void executeQuery(ICursorMeta meta, boolean isStreaming) throws SQLException {
        setContext(meta, isStreaming);
        SqlAndParam sqlAndParam = null;
        Connection con = null;
        boolean isControlSql = false;

        sqlAndParam = new SqlAndParam();
        if (plan instanceof IQuery && ((IQuery) plan).getSql() != null) {

            sqlAndParam.sql = ((IQuery) plan).getSql();
            sqlAndParam.param = new HashMap<Integer, ParameterContext>();
            isControlSql = true;

        } else {
            cursorMeta.setIsSureLogicalIndexEqualActualIndex(true);
            // buildRetColumns(oneQuery, meta);
            // sqlAndParam = oneQuery.buildSqlAndParam(true);
            if (plan instanceof IQueryTree) {
                ((IQueryTree) plan).setTopQuery(true);
                MysqlPlanVisitorImpl visitor = new MysqlPlanVisitorImpl((IQueryTree) plan, null, null, true);
                plan.accept(visitor);
                sqlAndParam.sql = visitor.getString();
                sqlAndParam.param = visitor.getParamMap();
            }
        }

        executionType = ExecutionType.PUT;
        con = getConnection();

        if (logger.isDebugEnabled()) {
            logger.warn("sqlAndParam:\n" + sqlAndParam);
        }

        ps = con.prepareStatement(sqlAndParam.sql);

        if (isStreaming) {
            // 当prev的时候 不能设置
            setStreamingForStatement(ps);
        }
        Map<Integer, ParameterContext> map = sqlAndParam.param;
        ParameterMethod.setParameters(ps, map);
        try {
            ResultSet rs = new ResultSetRemeberIfClosed(ps.executeQuery());
            if (isControlSql) {
                rs = new ResultSetAutoCloseConnection(rs, con, ps);
            }
            this.resultSet = rs;
        } catch (SQLException e) {
            if (e.getMessage()
                .contains("only select, insert, update, delete,replace,truncate,create,drop,load,merge sql is supported")) {
                ps = con.prepareStatement("select 1");
                this.resultSet = new ResultSetRemeberIfClosed(new ResultSetAutoCloseConnection(ps.executeQuery(),
                    con,
                    ps));

            } else {
                throw new RuntimeException("sql generated is :\n" + sqlAndParam.toString(), e);
            }
        }
    }

    protected void setStreamingForStatement(Statement stat) throws SQLException {
        stat.setFetchSize(Integer.MIN_VALUE);

        if (logger.isDebugEnabled()) {
            logger.warn("fetchSize:\n" + stat.getFetchSize());
        }
    }

    protected void setContext(ICursorMeta meta, boolean isStreaming) {
        if (cursorMeta == null) {
            cursorMeta = meta;
        }

        if (isStreaming != this.isStreaming) {
            this.isStreaming = isStreaming;
        }
    }

    public boolean isAutoCommit() throws SQLException {
        return myTransaction.autoCommit;
    }

    public void executeUpdate(ExecutionContext executionContext, IPut put, ITable table, IndexMeta meta)
                                                                                                        throws SQLException {

        MysqlPlanVisitorImpl visitor = new MysqlPlanVisitorImpl(put, null, null, true);
        put.accept(visitor);

        SqlAndParam sqlAndParam = new SqlAndParam();
        sqlAndParam.sql = visitor.getString();
        sqlAndParam.param = visitor.getParamMap();

        ps = prepareStatement(sqlAndParam.sql, getConnection());
        ParameterMethod.setParameters(ps, sqlAndParam.param);

        if (logger.isDebugEnabled()) {
            logger.warn("sqlAndParam:\n" + sqlAndParam);
        }
        int tmpNum = ps.executeUpdate();
        UpdateResultWrapper urw = new UpdateResultWrapper(tmpNum, this);
        executionType = ExecutionType.PUT;
        this.resultSet = urw;
    }

    // private void putOperater(ExecutionContext executionContext, IPut put,
    // OnePut onePut, DataSource ds)
    // throws SQLException {
    // My_Transaction myTransaction = transaction(executionContext);
    // logger.warn("onePut:\n" + onePut);
    //
    // int count = 0;
    // for (Entry<String, Map<String, MyParamSqlContext>> tmp :
    // onePut.sqls.entrySet()) {
    // for (Entry<String, MyParamSqlContext> context :
    // tmp.getValue().entrySet()) {
    // if (count == 1) {
    // throw new IllegalArgumentException("should not be here");
    // }
    // count++;
    // Connection conn = null;
    // try {
    // conn = myTransaction.getConnection(tmp.getKey(), ds, true);
    // SqlAndParam sqlAndParam = new SqlAndParam();
    // sqlAndParam.sql = context.getValue().getSql();
    // sqlAndParam.param = context.getValue().param;
    // executeUpdate(sqlAndParam, conn);
    // } catch (Exception e) {
    // throw new RuntimeException(e);
    // }
    // }
    // }
    // }

    public My_Transaction transaction(ExecutionContext executionContext) throws SQLException {
        ITransaction tra = executionContext.getTransaction();
        My_Transaction myTrans = null;
        if (tra != null) {
            // 有事务，使用事务jdbcHandler
            if (!(tra instanceof My_Transaction)) {
                throw new SQLException("not my transaction ");
            }
            myTrans = (My_Transaction) tra;
        } else {// 无事务
            myTrans = new My_Transaction();
        }
        this.myTransaction = myTrans;
        executionContext.setTransaction(myTrans);
        return myTrans;
    }

    public void beginTransaction() throws SQLException {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
    }

    public void commit() throws SQLException {
        Connection conn = getConnection();
        conn.commit();
    }

    public void rollback() throws SQLException {
        Connection conn = getConnection();
        conn.rollback();
    }

    public Connection getConnection() throws SQLException {
        Connection con = myTransaction.getConnection(groupName, ds, strongConsistent);
        return con;
    }

    public DataSource getDs() {
        return ds;
    }

    public ISchematicCursor getResultCursor() {
        try {
            if (executionType == ExecutionType.PUT) {
                int i = resultSet.getInt(UpdateResultWrapper.AFFECT_ROW);
                ISchematicCursor isc = new AffectRowCursor(i);
                close();
                return isc;
            } else if (executionType == ExecutionType.GET) {
                // get的时候只会有一个结果集
                ResultSet rs = resultSet;
                return new ResultSetCursor(rs);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * (non-Javadoc)
     * @see com.taobao.ustore.jdbc.mysql.generalQueryHandler#getPs()
     */
    protected PreparedStatement getPs() {
        return ps;
    }

    /*
     * (non-Javadoc)
     * @see
     * com.taobao.ustore.jdbc.mysql.generalQueryHandler#setDs(javax.sql.DataSource
     * )
     */
    @Override
    public void setDs(Object ds) {
        this.ds = (DataSource) ds;
    }

    /*
     * (non-Javadoc)
     * @see com.taobao.ustore.jdbc.mysql.generalQueryHandler#close()
     */
    @Override
    public void close() throws SQLException {

        try {
            /*
             * 非流计算的时候，用普通的close方法，而如果是streaming的情况下 将按以下模式关闭：
             * http://jira.taobao.ali.com/browse/ANDOR-149
             * http://gitlab.alibaba-inc.com/andor/issues/1835
             */
            // 这一期不做这个了
            if (!isStreaming) {
                if (resultSet != null && !resultSet.isClosed()) {
                    resultSet.close();
                    resultSet = null;
                }
            } else {
                try {
                    if (resultSet != null && !resultSet.isClosed()) {
                        My_Transaction.closeStreaming(myTransaction, groupName, ds, strongConsistent);
                    }
                } catch (Throwable e) {
                    logger.warn("", e);
                    if (resultSet != null && !resultSet.isClosed()) {
                        resultSet.close();
                        resultSet = null;
                    }
                }
            }

        } finally {

            try {
                if (ps != null) {

                    ps.setFetchSize(0);
                    ps.close();
                    ps = null;
                }

            } finally {

                try {
                    myTransaction.close();
                } catch (TddlException e) {
                    SQLException sqlException = new SQLException(e);
                    throw sqlException;
                }
            }
        }
        executionType = null;
    }

    public void closeResultSetAndConnection() throws SQLException {

        try {
            /*
             * 非流计算的时候，用普通的close方法，而如果是streaming的情况下 将按以下模式关闭：
             * http://jira.taobao.ali.com/browse/ANDOR-149
             * http://gitlab.alibaba-inc.com/andor/issues/1835
             */
            // 这一期不做这个了
            if (!isStreaming) {
                if (resultSet != null && !resultSet.isClosed()) {
                    resultSet.close();
                    resultSet = null;
                }
            } else {
                try {
                    if (resultSet != null && !resultSet.isClosed()) {
                        My_Transaction.closeStreaming(myTransaction, groupName, ds, strongConsistent);
                    }
                } catch (Throwable e) {
                    logger.warn("", e);
                    if (resultSet != null && !resultSet.isClosed()) {
                        resultSet.close();
                        resultSet = null;
                    }
                }
            }

        } finally {

            try {
                if (ps != null) {

                    ps.setFetchSize(0);
                    ps.close();
                    ps = null;
                }

            } finally {

            }
        }
        executionType = null;
    }

    /*
     * (non-Javadoc)
     * @see
     * com.taobao.ustore.jdbc.mysql.generalQueryHandler#skipTo(com.taobao.ustore
     * .common.inner.bean.CloneableRecord,
     * com.taobao.ustore.common.inner.ICursorMeta)
     */
    @Override
    public boolean skipTo(CloneableRecord key, ICursorMeta indexMeta) throws SQLException {

        checkInitedInRsNext();
        throw new RuntimeException("暂时不支持skip to");
        // while (getResultSet().next()) {
        // for (String keyName : key.getColumnList()) {
        // Object v = key.get(keyName);
        // Object v2 = getResultSet().getObject(keyName);
        // if (v.equals(v2)) {
        // current = My_Convertor.convert(getResultSet(), indexMeta);
        // return true;
        // }
        // }
        // }
        // return false;
    }

    /*
     * (non-Javadoc)
     * @see com.taobao.ustore.jdbc.mysql.generalQueryHandler#next()
     */
    @Override
    public IRowSet next() throws SQLException {
        if (ds == null) {
            throw new TddlRuntimeException("数据源为空");
        }

        checkInitedInRsNext();

        prev_kv = current;
        try {
            if (resultSet.isClosed()) return null;
        } catch (Exception ex) {
            return null;
        }
        if (resultSet.next()) {
            current = My_Convertor.convert(resultSet, cursorMeta);
        } else {
            current = null;
        }

        return current;
    }

    /*
     * (non-Javadoc)
     * @see
     * com.taobao.ustore.jdbc.mysql.generalQueryHandler#prepareStatement(java
     * .lang.String)
     */
    protected PreparedStatement prepareStatement(String sql, Connection myJdbcHandler) throws SQLException {
        if (this.ps != null) {
            throw new IllegalStateException("上一个请求还未执行完毕");
        }
        if (myJdbcHandler == null) {
            throw new IllegalStateException("should not be here");
        }
        PreparedStatement ps = myJdbcHandler.prepareStatement(sql);
        this.ps = ps;

        return ps;
    }

    /*
     * (non-Javadoc)
     * @see com.taobao.ustore.jdbc.mysql.generalQueryHandler#first()
     */
    @Override
    public IRowSet first() throws SQLException {
        resultSet.beforeFirst();
        resultSet.next();
        current = My_Convertor.convert(resultSet, cursorMeta);
        return current;

    }

    /*
     * (non-Javadoc)
     * @see com.taobao.ustore.jdbc.mysql.generalQueryHandler#last()
     */
    @Override
    public IRowSet last() throws SQLException {
        resultSet.afterLast();
        resultSet.previous();
        current = My_Convertor.convert(resultSet, cursorMeta);
        return current;

    }

    /*
     * (non-Javadoc)
     * @see com.taobao.ustore.jdbc.mysql.generalQueryHandler#getCurrent()
     */
    @Override
    public IRowSet getCurrent() {
        return current;
    }

    protected void checkInitedInRsNext() {
        if (!isInited()) {
            throw new IllegalArgumentException("not inited");
        }
    }

    /*
     * (non-Javadoc)
     * @see com.taobao.ustore.jdbc.mysql.generalQueryHandler#isInited()
     */
    @Override
    public boolean isInited() {
        return executionType != null;
    }

    public void beforeFirst() throws SQLException {
        this.closeResultSetAndConnection();
        this.executeQuery(cursorMeta, isStreaming);
        current = null;
    }

    boolean                   initPrev = false;
    private IDataNodeExecutor plan;

    /*
     * (non-Javadoc)
     * @see com.taobao.ustore.jdbc.mysql.generalQueryHandler#prev()
     */
    @Override
    public IRowSet prev() throws SQLException {
        if (ds == null) {

            throw new TddlRuntimeException("数据源为空");
        }
        if (!initPrev) {
            initPrev = true;
            return convertRowSet(resultSet.last());

        }
        checkInitedInRsNext();

        return convertRowSet(resultSet.previous());

    }

    protected IRowSet convertRowSet(boolean isOk) throws SQLException {
        prev_kv = current;
        if (isOk) {
            current = My_Convertor.convert(resultSet, cursorMeta);
        } else {
            current = null;
        }

        return current;
    }

    /*
     * (non-Javadoc)
     * @see com.taobao.ustore.jdbc.mysql.generalQueryHandler#isDone()
     */
    @Override
    public boolean isDone() {
        return true;
    }

    public IRowSet next(long timeout, TimeUnit unit) throws SQLException {
        // TODO shenxun : 如果真的需要可能需要用外部线程interrupted掉网络io等待。
        return next();
    }

    @Override
    public void cancel(boolean interruptedIfRunning) {

    }

    public void setMyTransaction(My_Transaction myTransaction) {
        this.myTransaction = myTransaction;
    }

    @Override
    public boolean isCanceled() {

        return false;
    }

    public boolean isStrongConsistent() {
        return strongConsistent;
    }

    public void setStrongConsistent(boolean strongConsistent) {
        this.strongConsistent = strongConsistent;
    }

    public My_Transaction getMyTransaction() {
        return myTransaction;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public void setDs(DataSource ds) {
        this.ds = ds;
    }

    public Boolean getIsStreaming() {
        return isStreaming;
    }

    public void setIsStreaming(Boolean isStreaming) {
        this.isStreaming = isStreaming;
    }

    public ResultSet getResultSet() {
        return this.resultSet;
    }

    public void setPlan(IDataNodeExecutor plan) {
        this.plan = plan;

    }

    public IDataNodeExecutor getPlan() {
        return this.plan;
    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    public void setExecutionContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

}
