package com.taobao.tddl.repo.bdb.spi;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.ExceptionErrorCodeUtils;
import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.codec.RecordCodec;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.TransactionConfig.Isolation;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.spi.AbstractTable;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

/**
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class JE_Table extends AbstractTable {

    boolean               isTempTable = false;
    Map<String, Database> databases   = new HashMap();

    public Map<String, Database> getDatabases() {
        return databases;
    }

    public void setDatabases(Map<String, Database> databases) {
        this.databases = databases;
    }

    Map<String/** index **/
    , KVCodec>    indexCodecMap;
    DatabaseEntry emptyValueEntry = new DatabaseEntry();

    {
        emptyValueEntry.setData(new byte[1]);
    }

    public JE_Table(TableMeta schema, IRepository repo){
        super(schema, repo);
        indexCodecMap = new HashMap<String, KVCodec>();

        KVCodec pkCodec = null;
        IndexMeta pkIndex = getSchema().getPrimaryIndex();

        if (pkCodec == null) {
            pkCodec = new KVCodec();
            pkCodec.setKey_codec(CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
                .getCodec((pkIndex.getKeyColumns())));
            if (pkIndex.getValueColumns() != null) {
                pkCodec.setValue_codec(CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
                    .getCodec((pkIndex.getValueColumns())));
            }
        }

        indexCodecMap.put(pkIndex.getName(), pkCodec);

        for (IndexMeta secondIndex : getSchema().getSecondaryIndexes()) {
            KVCodec secCodec = new KVCodec();
            secCodec.setKey_codec(CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
                .getCodec((secondIndex.getKeyColumns())));
            if (secondIndex.getValueColumns() != null) {
                secCodec.setValue_codec(CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
                    .getCodec((secondIndex.getValueColumns())));
            }
            indexCodecMap.put(secondIndex.getName(), secCodec);
        }

    }

    public Database getDatabase(String name) {

        Database db = databases.get(name);

        if (db == null) {
            synchronized (this) {
                db = databases.get(name);

                if (db == null) {
                    db = ((JE_Repository) this.repo).getDatabase(name, schema.isTmp(), schema.issortedDuplicates());
                    databases.put(name, db);
                }

            }
        }

        return db;

    }

    public ISchematicCursor getCursor(ITransaction txn, IndexMeta indexMeta, String isolation, String actualTableName)
                                                                                                                      throws TddlException {
        Database db = getDatabase(actualTableName);
        if (db == null) {
            throw new TddlException("table don't contains indexName:" + actualTableName);
        }
        CursorConfig cc = CursorConfig.DEFAULT;
        LockMode lm = LockMode.DEFAULT;
        if (txn != null) {
            com.sleepycat.je.TransactionConfig _config = ((JE_Transaction) txn).config;
            if (_config.getReadUncommitted()) {
                cc = CursorConfig.READ_UNCOMMITTED;
                lm = LockMode.READ_UNCOMMITTED;
            } else if (_config.getReadCommitted()) {
                cc = CursorConfig.READ_COMMITTED;
                // lm = LockMode.READ_COMMITTED;
            }
        } else {
            if (Isolation.READ_COMMITTED.equals(isolation)) {
                cc = CursorConfig.READ_COMMITTED;
                // lm = LockMode.READ_COMMITTED;//not support
            } else if (Isolation.READ_UNCOMMITTED.equals(isolation)) {
                cc = CursorConfig.READ_UNCOMMITTED;
                lm = LockMode.READ_UNCOMMITTED;
            } else if (Isolation.REPEATABLE_READ.equals(isolation)) {
                // default
            } else if (Isolation.SERIALIZABLE.equals(isolation)) {
                // txn_config
            }
        }
        JE_Cursor je_cursor = new JE_Cursor(indexMeta,

        db.openCursor(txn == null ? null : ((JE_Transaction) txn).txn, cc), lm);
        if (txn != null) {
            ((JE_Transaction) txn).addCursor(je_cursor);
        }
        return new SchematicCursor(je_cursor, je_cursor.getiCursorMeta(), ExecUtils.getOrderBy(indexMeta));
    }

    /**
     * todo:触发更新二级索引
     * 
     * @param key
     * @param value
     * @throws TddlException
     */
    @Override
    public void put(ExecutionContext context, CloneableRecord key, CloneableRecord value, IndexMeta indexMeta,
                    String dbName) throws TddlException {
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry valueEntry = new DatabaseEntry();
        keyEntry.setData(indexCodecMap.get(indexMeta.getName()).getKey_codec().encode(key));

        // 当临时表排序时候可能会用到临时表join，主键插入值为null
        // if (keyEntry.getData().length == 1 && !isTempTable) {
        // throw new RuntimeException("pk must not null.");
        // }

        if (value != null) {
            valueEntry.setData(indexCodecMap.get(indexMeta.getName()).getValue_codec().encode(value));
        } else {
            valueEntry = emptyValueEntry;
        }
        try {
            ITransaction transaction = context.getTransaction();
            com.sleepycat.je.Transaction txn = null;
            if (transaction != null && transaction instanceof JE_Transaction) {
                txn = ((JE_Transaction) transaction).txn;
            }
            OperationStatus operationStatus = getDatabase(dbName).put(txn, keyEntry, valueEntry);
            if (operationStatus.equals(OperationStatus.SUCCESS)) {
                return;
            }
        } catch (LockTimeoutException ex) {

            throw ex;
        } catch (ReplicaWriteException ex) {
            throw new TddlException(ExceptionErrorCodeUtils.Read_only, ex);
        }
    }

    // @Override
    // public long count() {
    // return databases.get(getSchema().getPrimaryIndex().getName()).count();
    // }
    //
    // @Override
    // public void sync() {
    // for (Entry<String, Database> entry : databases.entrySet()) {
    // entry.getValue().sync();
    // }
    // }

    @Override
    public void close() {
        for (Entry<String, Database> entry : databases.entrySet()) {
            entry.getValue().close();
        }
    }

    @Override
    public void delete(ExecutionContext context, CloneableRecord key, IndexMeta indexMeta, String dbName)
                                                                                                         throws TddlException {

        DatabaseEntry keyEntry = new DatabaseEntry();
        keyEntry.setData(indexCodecMap.get(indexMeta.getName()).getKey_codec().encode(key));
        try {
            getDatabase(dbName).delete(context.getTransaction() == null ? null : ((JE_Transaction) context.getTransaction()).txn,
                keyEntry);
        } catch (LockTimeoutException ex) {

            throw ex;
        } catch (ReplicaWriteException ex) {
            throw new TddlException(ExceptionErrorCodeUtils.Read_only, ex);
        }
    }

    @Override
    public CloneableRecord get(ExecutionContext context, CloneableRecord key, IndexMeta indexMeta, String dbName) {

        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry valueEntry = new DatabaseEntry();
        keyEntry.setData(indexCodecMap.get(indexMeta.getName()).getKey_codec().encode(key));
        OperationStatus status = getDatabase(dbName).get(context.getTransaction() == null ? null : ((JE_Transaction) context.getTransaction()).txn,
            keyEntry,
            valueEntry,
            LockMode.DEFAULT);
        if (OperationStatus.SUCCESS != status) {
            return null;
        }
        if (valueEntry.getSize() != 0) {
            return indexCodecMap.get(indexMeta.getName()).getValue_codec().decode(valueEntry.getData(), false);
        } else {
            return null;
        }
    }

    public void setTempTable(boolean isTempTable) {
        this.isTempTable = isTempTable;
    }

    @Override
    public ISchematicCursor getCursor(ExecutionContext executionContext, IndexMeta meta, IQuery iQuery)
                                                                                                       throws TddlException {
        String actualTable = iQuery.getTableName();
        return getCursor(executionContext.getTransaction(), meta, executionContext.getIsolation(), actualTable);
    }

    @Override
    public ISchematicCursor getCursor(ExecutionContext executionContext, IndexMeta indexMeta, String actualTableName)
                                                                                                                     throws TddlException {
        Database db = getDatabase(actualTableName);
        if (db == null) {
            throw new TddlException("table don't contains indexName:" + actualTableName);
        }
        ITransaction txn = executionContext.getTransaction();
        CursorConfig cc = CursorConfig.DEFAULT;
        LockMode lm = LockMode.DEFAULT;
        if (txn != null) {
            com.sleepycat.je.TransactionConfig _config = ((JE_Transaction) txn).config;
            if (_config.getReadUncommitted()) {
                cc = CursorConfig.READ_UNCOMMITTED;
                lm = LockMode.READ_UNCOMMITTED;
            } else if (_config.getReadCommitted()) {
                cc = CursorConfig.READ_COMMITTED;
                // lm = LockMode.READ_COMMITTED;
            }
        } else {

            cc = CursorConfig.READ_COMMITTED;

        }
        JE_Cursor je_cursor = new JE_Cursor(indexMeta,

        db.openCursor(txn == null ? null : ((JE_Transaction) txn).txn, cc), lm);
        if (txn != null) {
            ((JE_Transaction) txn).addCursor(je_cursor);
        }
        return new SchematicCursor(je_cursor, je_cursor.getiCursorMeta(), ExecUtils.getOrderBy(indexMeta));
    }
}

class KVCodec {

    RecordCodec key_codec;
    RecordCodec value_codec;

    public RecordCodec getKey_codec() {
        return key_codec;
    }

    public void setKey_codec(RecordCodec key_codec) {
        this.key_codec = key_codec;
    }

    public RecordCodec getValue_codec() {
        return value_codec;
    }

    public void setValue_codec(RecordCodec value_codec) {
        this.value_codec = value_codec;
    }
}
