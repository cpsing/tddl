package com.taobao.tddl.repo.mysql.spi;

import java.sql.SQLException;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.plan.IPut;

/**
 * 集中所有jdbc操作到这个Handler. 这样才能支持同步化和异步化执行
 * 
 * @author Whisper 2013-6-20 上午9:32:46
 * @since 3.0.1
 */
public interface GeneralQueryHandler {

    /**
     * 初始化结果集
     * 
     * @param oneQuery
     * @param meta
     * @param isStreaming
     * @throws SQLException
     */
    public abstract void executeQuery(ICursorMeta meta, boolean isStreaming) throws SQLException;

    /**
     * 自动提交？
     * 
     * @return
     * @throws SQLException
     */
    public abstract boolean isAutoCommit() throws SQLException;

    /**
     * 获取结果集
     * 
     * @return
     */
    public abstract ISchematicCursor getResultCursor();

    /**
     * 设置dataSource
     * 
     * @param ds
     */
    public abstract void setDs(Object ds);

    /**
     * 关闭,但不是真关闭
     * 
     * @throws SQLException
     */
    public abstract void close() throws SQLException;

    /**
     * 跳转（不过大部分指针都不支持这个）
     * 
     * @param key
     * @param indexMeta
     * @return
     * @throws Exception
     */
    public abstract boolean skipTo(CloneableRecord key, ICursorMeta indexMeta) throws SQLException;

    /**
     * 指针下移 返回null则表示没有后项了
     * 
     * @return
     * @throws Exception
     */
    public abstract IRowSet next() throws SQLException;

    /**
     * 最开始的row
     * 
     * @return
     * @throws Exception
     */
    public abstract IRowSet first() throws SQLException;

    /**
     * 最后一个row
     * 
     * @return
     * @throws Exception
     */
    public abstract IRowSet last() throws SQLException;

    /**
     * 获取当前row
     * 
     * @return
     */
    public abstract IRowSet getCurrent();

    /**
     * 是否触发过initResultSet()这个方法。 与isDone不同的是这个主要是防止多次提交用。（估计）
     * 
     * @return
     */
    public abstract boolean isInited();

    /**
     * 获取上一个数据
     * 
     * @return
     * @throws Exception
     */
    public abstract IRowSet prev() throws SQLException;

    /**
     * 执行一个update语句
     * 
     * @param sqlAndParam
     * @return
     * @throws SQLException
     */
    public void executeUpdate(ExecutionContext executionContext, IPut put, ITable table, IndexMeta meta)
                                                                                                        throws SQLException;

    /**
     * 用于异步化，等同于Future.isDone();
     * 
     * @return
     */
    public abstract boolean isDone();

    /**
     * 用于异步化，等同于Future.cancel();
     */
    public abstract void cancel(boolean mayInterruptIfRunning);

    /**
     * 用于异步化，等同于Future.isCancel();
     * 
     * @return
     */
    public abstract boolean isCanceled();

    /**
     * 指针前移
     * 
     * @throws SQLException
     */
    public void beforeFirst() throws SQLException;

}
