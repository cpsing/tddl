package com.taobao.tddl.executor.spi;

import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

/**
 * 表操作
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午3:59:42
 * @since 5.0.0
 */
public interface ITable {

    /**
     * 获取表的描述信息数据。
     * 
     * @return
     */
    TableMeta getSchema() throws TddlException;

    /**
     * 核心写接口
     * 
     * @param txn
     * @param key
     * @param value
     * @throws Exception
     */
    void put(ExecutionContext executionContext, CloneableRecord key, CloneableRecord value, IndexMeta indexMeta,
             String dbName) throws TddlException;

    /**
     * 关闭table
     */
    void close() throws TddlException;

    /**
     * 删除一行数据
     * 
     * @param txn
     * @param key
     * @throws Exception
     */
    void delete(ExecutionContext executionContext, CloneableRecord key, IndexMeta indexMeta, String dbName)
                                                                                                           throws TddlException;

    /**
     * 获取一行数据。
     * 
     * @param txn
     * @param key
     * @return
     */
    CloneableRecord get(ExecutionContext executionContext, CloneableRecord key, IndexMeta indexMeta, String dbName)
                                                                                                                   throws TddlException;

    /**
     * 根据meta 源信息，获取一个表数据指针。这个指针应该是delegate的，这个方法不应该耗费太多资源。 cursor的隔离性
     * 
     * @param txn
     * @param meta
     * @param isolation
     * @param iQuery
     * @return
     * @throws FetchException
     * @throws SQLException
     */
    ISchematicCursor getCursor(ExecutionContext executionContext, IndexMeta meta, IQuery iQuery) throws TddlException;

    ISchematicCursor getCursor(ExecutionContext executionContext, IndexMeta indexMeta, String indexMetaName)
                                                                                                            throws TddlException;
}
