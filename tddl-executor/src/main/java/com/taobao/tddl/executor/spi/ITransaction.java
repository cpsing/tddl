package com.taobao.tddl.executor.spi;

import com.taobao.tddl.common.exception.TddlException;

/**
 * 事务对象
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午4:00:49
 * @since 5.0.0
 */
public interface ITransaction {

    long getId();

    void commit() throws TddlException;

    void rollback() throws TddlException;

    boolean isAutoCommit();

    public ITHLog getHistoryLog();

    public void close() throws TddlException;

    public void setAutoCommit(boolean autoCommit);

}
