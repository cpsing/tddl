package com.taobao.tddl.repo.bdb.spi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.ExceptionErrorCodeUtils;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.codec.RecordCodec;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.cursor.Cursor;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;

/**
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class JE_Cursor implements Cursor {

    com.sleepycat.je.Cursor je_cursor;
    DatabaseEntry           key             = new DatabaseEntry();
    DatabaseEntry           value           = new DatabaseEntry();
    LockMode                lockMode;
    IndexMeta               indexMeta;
    KVPair                  current         = new KVPair();
    RecordCodec             key_codec;
    RecordCodec             value_codec;
    CloneableRecord         key_record;
    CloneableRecord         value_record;
    boolean                 reuse           = false;
    ICursorMeta             iCursorMeta     = null;
    DatabaseEntry           emptyValueEntry = new DatabaseEntry();
    private boolean         isBeforeFirst   = false;

    {
        emptyValueEntry.setData(new byte[1]);
    }

    public JE_Cursor(IndexMeta meta, com.sleepycat.je.Cursor je_cursor, LockMode lockMode){
        this.indexMeta = meta;
        this.iCursorMeta = ExecUtils.convertToICursorMeta(meta);
        this.je_cursor = je_cursor;
        this.lockMode = lockMode;
        this.key_codec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(meta.getKeyColumns());
        if (meta.isPrimaryKeyIndex()) {
            this.value_codec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(meta.getValueColumns());
        } else {
            this.value_codec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(meta.getValueColumns());
        }
        this.key_record = key_codec.newEmptyRecord();
        this.value_record = value_codec.newEmptyRecord();
    }

    public static IRowSet convertToIRowSet(ICursorMeta indexMeta, KVPair kvPair) {
        if (kvPair == null) {
            return null;
        }
        return new RowSetKVPairImp(indexMeta, kvPair);
    }

    @Override
    public IRowSet next() {
        if (isBeforeFirst) {
            isBeforeFirst = false;
            return convertToIRowSet(iCursorMeta, current);
        }
        OperationStatus status = je_cursor.getNext(key, value, lockMode);
        if (OperationStatus.SUCCESS == status) {

            // 任何情况都应该返回新的KVPair，要不然会覆盖上一次结果
            // if (current == null) {
            current = new KVPair();
            // }
            current.setKey(key_codec.decode(key.getData(), reuse));
            if (value.getSize() == 0) {
                current.setValue(null);
            } else {
                current.setValue(value_codec.decode(value.getData(), false));
            }
        } else {
            current = null;
        }
        return convertToIRowSet(iCursorMeta, current);
    }

    @Override
    public boolean skipTo(CloneableRecord skip_key) {
        key.setData(key_codec.encode(skip_key));
        if (OperationStatus.SUCCESS == je_cursor.getSearchKeyRange(key, value, lockMode)) {
            setKV();
            // CloneableRecord cr = current.getKey();
            // boolean equals = isEquals(skip_key, cr);
            // if(!equals)
            // {//当SearchKeyRange 取到的数据
            // 不等于当前数据的时候，实际上指针是在数据之前的。所以需要额外next一次。不过不确定。。有问题再说吧。。
            // next();
            // }
            // //System.out.println("skip true:"+skip_key+",,,current="+current);
            return true;
        }
        // //System.out.println("skip false:"+skip_key);
        return false;
    }

    private boolean isEquals(CloneableRecord right, CloneableRecord left) {
        boolean equals = true;
        List<String> leftColumns = left.getColumnList();
        List<String> rightColumns = right.getColumnList();
        Iterator<String> leftIter = leftColumns.iterator();
        Iterator<String> rightIter = rightColumns.iterator();
        while (leftIter.hasNext()) {
            if (!rightIter.hasNext()) {
                equals = false;
                break;
            } else {
                String leftCol = leftIter.next();
                String rightCol = rightIter.next();

                Object leftVal = left.get(leftCol);
                Object rightVal = right.get(rightCol);

                if (leftVal != null && !leftVal.equals(rightVal)) {
                    equals = false;
                    break;
                }
                if (leftVal == null && rightVal != null) {
                    equals = false;
                    break;
                }
            }
        }
        // 如果左面耗尽，右面没有耗尽，那么也认为不等于。
        if (rightIter.hasNext()) {
            equals = false;
        }
        return equals;
    }

    public boolean skipToNoRange(CloneableRecord skip_key) {
        key.setData(key_codec.encode(skip_key));
        OperationStatus status = je_cursor.getSearchKey(key, value, lockMode);
        if (OperationStatus.SUCCESS == status) {
            setKV();
            // //System.out.println("skip true:"+skip_key+",,,current="+current);
            return true;
        }
        // //System.out.println("skip false:"+skip_key);
        return false;
    }

    @Override
    public List<TddlException> close(List<TddlException> exs) {
        if (exs == null) exs = new ArrayList();
        try {
            je_cursor.close();
        } catch (Exception e) {
            exs.add(new TddlException(e));
        }
        return exs;
    }

    @Override
    public IRowSet prev() throws TddlException {
        OperationStatus status = je_cursor.getPrev(key, value, lockMode);
        if (OperationStatus.SUCCESS == status) {
            setKV();
            return convertToIRowSet(iCursorMeta, current);
        } else {
            return null;
        }
    }

    @Override
    public IRowSet first() throws TddlException {
        OperationStatus status = je_cursor.getFirst(key, value, lockMode);
        if (OperationStatus.SUCCESS == status) {
            setKV();
            return convertToIRowSet(iCursorMeta, current);
        } else {
            return null;
        }
    }

    @Override
    public IRowSet last() throws TddlException {
        OperationStatus status = je_cursor.getLast(key, value, lockMode);
        if (OperationStatus.SUCCESS == status) {
            setKV();
            return convertToIRowSet(iCursorMeta, current);
        } else {
            return null;
        }
    }

    @Override
    public IRowSet current() throws TddlException {
        if (current == null || current.getKey() == null) {
            OperationStatus status = je_cursor.getNext(key, value, lockMode);
            if (OperationStatus.SUCCESS == status) {
                setKV();
                return convertToIRowSet(iCursorMeta, current);
            } else {
                return null;
            }
        } else {
            return convertToIRowSet(iCursorMeta, current);
        }
    }

    @Override
    public boolean delete() throws TddlException {
        return OperationStatus.SUCCESS == je_cursor.delete();
    }

    private void setKV() {
        current = new KVPair();

        current.setKey(key_codec.decode(key.getData(), reuse));
        if (value.getSize() == 0) {
            current.setValue(null);
        } else {
            current.setValue(value_codec.decode(value.getData(), false));
        }
    }

    // public KVPair get(CloneableRecord search_key) throws TddlException {
    // key.setData(key_codec.encode(search_key));
    // OperationStatus status = je_cursor.getSearchKey(key, value, lockMode);
    // if (OperationStatus.SUCCESS == status) {
    // setKV();
    // } else {
    // current = null;
    // }
    // return current;
    // }

    @Override
    public boolean skipTo(KVPair kv) throws TddlException {
        /*
         * for (ColumnMeta c : meta.getKeyColumns()) { Object v =
         * kv.getKey().get(c.getName()); if (v == null) { if(kv.getValue() !=
         * null) v = kv.getValue().get(c.getName()); }
         * key_record.put(c.getName(), v); } return skipTo(key_record);
         */
        key.setData(key_codec.encode(kv.getKey()));
        if (value != null) {
            value.setData(value_codec.encode(kv.getValue()));
        }
        if (OperationStatus.SUCCESS == je_cursor.getSearchBothRange(key, value, lockMode)) {
            setKV();
            // //System.out.println("skip true:"+skip_key+",,,current="+current);
            return true;
        }
        // //System.out.println("skip false:"+skip_key);
        return false;
    }

    // public KVPair get(KVPair kv) throws TddlException {
    // for (ColumnMeta c : meta.getKeyColumns()) {
    // Object v = kv.getKey().get(c.getName());
    // if (v == null) {
    // if(kv.getValue() != null)
    // v = kv.getValue().get(c.getName());
    // }
    // key_record.put(c.getName(), v);
    // }
    // return get(key_record);
    // }

    @Override
    public IRowSet getNextDup() throws TddlException {
        OperationStatus status = je_cursor.getNextDup(key, value, lockMode);
        if (OperationStatus.SUCCESS == status) {
            setKV();
        } else {
            current = null;
        }
        return convertToIRowSet(iCursorMeta, current);
    }

    @Override
    public void put(CloneableRecord key, CloneableRecord value) throws TddlException {
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry valueEntry = new DatabaseEntry();
        keyEntry.setData(key_codec.encode(key));
        if (value != null) {
            valueEntry.setData(value_codec.encode(value));
        } else {
            valueEntry = emptyValueEntry;
        }
        try {
            je_cursor.put(keyEntry, valueEntry);
        } catch (LockTimeoutException ex) {
            LockTimeoutException ex1 = ex;
            while (ex1 != null) {
                try {
                    je_cursor.put(keyEntry, valueEntry);
                    ex1 = null;
                } catch (LockTimeoutException ex2) {
                    ex1 = ex2;
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        throw new TddlException(e);
                    }
                }
            }
        } catch (ReplicaWriteException ex) {
            throw new TddlException(ExceptionErrorCodeUtils.Read_only, ex);
        }
    }

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) throws TddlException {
        if (keyFilterOrValueFilter == false) throw new UnsupportedOperationException("BDB的valueFilter的mget还未实现");
        Map<CloneableRecord, DuplicateKVPair> retMap = new HashMap<CloneableRecord, DuplicateKVPair>(64);

        if (keys == null) {
            keys = Collections.emptyList();
        }
        for (CloneableRecord cr : keys) {

            if (prefixMatch) {
                throw new UnsupportedOperationException("not supported yet ");
            } else {
                dontPrefixMatch(retMap, cr);

            }
        }

        return retMap;
    }

    private void dontPrefixMatch(Map<CloneableRecord, DuplicateKVPair> retMap, CloneableRecord cr) throws TddlException {
        // 跳转到这个cr的第一个
        boolean has = this.skipToNoRange(cr);
        if (has) {
            DuplicateKVPair dkv = buildDuplicateKVPair();

            retMap.put(cr, dkv);
        } else {/*
                 * do nothing returnList.add(null);
                 */
        }
    }

    private void dontPrefixMatchList(List<DuplicateKVPair> retList, CloneableRecord cr) throws TddlException {
        // 跳转到这个cr的第一个
        boolean has = this.skipToNoRange(cr);
        if (has) {
            DuplicateKVPair dkv = buildDuplicateKVPair();

            retList.add(dkv);
        } else {/*
                 * do nothing returnList.add(null);
                 */
        }
    }

    private DuplicateKVPair buildDuplicateKVPair() throws TddlException {
        IRowSet kvPair = this.current();
        DuplicateKVPair dkv = new DuplicateKVPair(kvPair);
        while ((kvPair = getNextDup()) != null) {// 添加这这个cr的重复数据列
            dkv.next = new DuplicateKVPair(kvPair);
            dkv = dkv.next;
        }
        return dkv;
    }

    @Override
    public List<DuplicateKVPair> mgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                       boolean keyFilterOrValueFilter) throws TddlException {

        if (keyFilterOrValueFilter == false) throw new UnsupportedOperationException("BDB的valueFilter的mget还未实现");
        List<DuplicateKVPair> retList = new ArrayList<DuplicateKVPair>(64);

        if (keys == null) {
            keys = Collections.emptyList();
        }
        for (CloneableRecord cr : keys) {

            if (prefixMatch) {
                throw new UnsupportedOperationException("not supported yet ");
            } else {
                dontPrefixMatchList(retList, cr);

            }
        }

        return retList;
    }

    @Override
    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String tab = GeneralUtil.getTab(inden);
        sb.append(tab).append("【Je cursor : ").append("\n");
        if (indexMeta != null) {
            sb.append(indexMeta.toStringWithInden(inden + 1));
        }
        sb.append("\n");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    @Override
    public void beforeFirst() throws TddlException {
        this.isBeforeFirst = true;
        // 跳到第一个之前
        this.first();
    }

    public ICursorMeta getiCursorMeta() {
        return iCursorMeta;
    }

    public void setiCursorMeta(ICursorMeta iCursorMeta) {
        this.iCursorMeta = iCursorMeta;
    }

    List<ColumnMeta> returnColumns = null;

    @Override
    public List<ColumnMeta> getReturnColumns() throws TddlException {
        if (returnColumns == null) {
            returnColumns = new ArrayList(indexMeta.getKeyColumns().size() + indexMeta.getValueColumns().size());
            returnColumns.addAll(indexMeta.getKeyColumns());
            returnColumns.addAll(indexMeta.getValueColumns());
        }

        return returnColumns;
    }

    @Override
    public boolean isDone() {
        return true;
    }

}
