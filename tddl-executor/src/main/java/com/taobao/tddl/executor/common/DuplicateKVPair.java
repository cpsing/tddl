package com.taobao.tddl.executor.common;

import com.taobao.tddl.executor.rowset.IRowSet;

/**
 * 一组重复的值的链表。 所有在同一个链表内的数据，都具备一组相同的key(key的意思就是在IRowSet中用来作为排序字段的哪些数据)
 * 
 * @author Whisper
 */
public class DuplicateKVPair {

    public DuplicateKVPair(IRowSet currentKey){
        super();
        this.currentKey = currentKey;
    }

    public IRowSet         currentKey;
    public DuplicateKVPair next = null;
}
