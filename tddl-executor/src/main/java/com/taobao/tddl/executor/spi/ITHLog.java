package com.taobao.tddl.executor.spi;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.plan.IPut;

/**
 * 写出log的东西
 * 
 * @author whisper
 */
public interface ITHLog {

    public void parepare(long transId, TableMeta table, IPut.PUT_TYPE putType, IRowSet oldRow, KVPair newRow)
                                                                                                             throws TddlRuntimeException;

    public void rollback(ITransaction trans) throws TddlRuntimeException;

    public void commit(ITransaction trans) throws TddlRuntimeException;
}
