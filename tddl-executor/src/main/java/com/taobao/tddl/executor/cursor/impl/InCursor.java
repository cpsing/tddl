package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.codec.RecordCodec;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.cursor.Cursor;
import com.taobao.tddl.executor.cursor.IInCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.record.FixedLengthRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;

/**
 * 专门处理 id in (xx,xx,xx,xx)的cursor 接受一大批的id in请求，然后分批，batch的方式，调用mget拿到具体的值。
 * 
 * @author whisper
 */
public class InCursor extends SchematicCursor implements IInCursor {

    /**
     * 单次请求，要设计一个限制,一次取出量不能太大。 TODO shenxun
     * :目前实现，是限制性的，下次可以设计成分批取多次的模式，以平衡性能和内存消耗。
     * 我一看一坨代码的东西就想哭啊。。妈的，所以简单实现一下，估计目前用不到这么复杂的算法
     */
    public int                sizeInOneGo        = 4000;
    IColumn                   c                  = null;
    /**
     * 需要查找的数据
     */
    List<Object>              valuesToFind       = null;
    OPERATION                 op                 = null; ;
    RecordCodec               keyCodec;

    IRowSet                   current            = null;

    /**
     * 一组返回的记录（size应该<=sizeInOneGo)，分批取出
     */
    List<DuplicateKVPair>     pairToReturn;

    /**
     * 一组返回记录的指针
     */
    Iterator<DuplicateKVPair> pairToReturnIterator;

    /**
     * 如果有相同的数据，那么一次取出后。 将这个值缓存在这里，下次可以取出。
     */
    DuplicateKVPair           duplicatePairCache = null;

    public InCursor(Cursor cursor, List<IOrderBy> orderBys, IColumn c, List<Object> v, OPERATION op){
        super(cursor, null, orderBys);
        keyCodec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
            .getCodec(Arrays.asList(ExecUtils.getColumnMeta(c)));
        this.c = c;

        // if (c.getDataType() == DATA_TYPE.DATE_VAL) {
        // List<Object> vNew = new ArrayList<Object>(v.size());
        // for (Object comp : v) {
        // vNew.add(new Date((Long) comp));
        // }
        // v = vNew;
        // }
        this.valuesToFind = v;
        this.op = op;
    }

    @Override
    protected void init() throws TddlException {
        super.init();
    }

    @Override
    protected void checkInited() throws TddlException {
        super.checkInited();
    }

    @Override
    public boolean skipTo(CloneableRecord key) throws TddlException {
        throw new IllegalArgumentException("should not be here");
    }

    @Override
    public boolean skipTo(KVPair key) throws TddlException {
        throw new IllegalArgumentException("should not be here");
    }

    public CloneableRecord getCloneableRecordOfKey(Object keyVal, RecordCodec keyCodec) {
        List cs = new ArrayList(1);
        cs.add(c);

        FixedLengthRecord record = new FixedLengthRecord(ExecUtils.convertISelectablesToColumnMeta(cs));
        record.put(this.c.getColumnName(), keyVal);
        return record;
    }

    @Override
    public IRowSet current() throws TddlException {
        return current;
    }

    Iterator<KVPair> duplicatePairIterator = null;

    @Override
    public IRowSet next() throws TddlException {
        if (valuesToFind == null) {
            throw new IllegalArgumentException("value is null ");
        }
        if (pairToReturnIterator == null) {
            // 还未初始化
            List<CloneableRecord> list = new ArrayList<CloneableRecord>(valuesToFind.size());
            for (Object valOne : valuesToFind) {
                list.add(getCloneableRecordOfKey(valOne, keyCodec));
            }
            pairToReturn = cursor.mgetWithDuplicateList(list, false, true);
            pairToReturnIterator = pairToReturn.iterator();
        }
        /**
         * 需要查找数据的iterator,用来记录指针
         */
        // Iterator<Comparable> valuesToFindIterator = valuesToFind.iterator();

        /*
         * 简单来说，就是根据sizeInOneGo,从values里面取出指定多的数据。 然后运行一次mget,将结果缓存在当前cursor里面。
         * 提供给外部进行next调用，应该之需要支持next和current吧。暂时 因为数据可能包含多个，比如1->0,1->2 ,
         * 1->3,2->0,2->1这样的数据，应该先将1对应的所有数据全部取尽，再去取第二个。
         */

        if (duplicatePairCache == null) {// 两中情况，一种是duplicate值为空，这时候应该尝试让pairToReturnIterator指针下移。
            // 如pairToReturnIterator也没有数据。则结果集内没有数据,直接返回空。
            if (pairToReturnIterator.hasNext()) {
                duplicatePairCache = pairToReturnIterator.next();
            } else {
                // while end/当前一批没有可以取的数据了,未来可以改成多层循环，一次batch取一小部分。
                setCurrent(null);
                return null;
            }
        }
        // 取当前值，可能是重复数据的第一个数据，或者也可能是重复数据中，被当前duplicatePairCache 缓存的数据
        setCurrent(duplicatePairCache.currentKey);
        // 指针下移
        duplicatePairCache = duplicatePairCache.next;
        return current;
    }

    public void setCurrent(IRowSet kvPair) {
        if (kvPair != null) {
            current = kvPair;
        }

    }

    /*
     * // 循环遍历，直到取出一个结果或取尽为止 DuplicateKVPair duplicate =
     * pairToReturnIterator.next(); if (duplicate == null) { setCurrent(null);
     * return null; } else { if (duplicatePairIterator == null) {//
     * 第一次进入当前可重复KVPair内。初始化iterator duplicatePairIterator =
     * duplicate.getKvPairs() .iterator(); } if
     * (duplicatePairIterator.hasNext()) {// 当前DuplicatePair // 有重复数据（或第一个数据）
     * KVPair ret = duplicatePairIterator.next(); setCurrent(ret); return ret; }
     * else { duplicatePairIterator = null; } } (non-Javadoc)
     * @see com.taobao.ustore.common.inner.AbstractCursor#prev()
     */

    @Override
    public boolean delete() throws TddlException {
        throw new IllegalArgumentException("should not be here");
    }

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) throws TddlException {
        return parentCursorMgetWithDuplicate(keys, prefixMatch, keyFilterOrValueFilter);
    }

}
