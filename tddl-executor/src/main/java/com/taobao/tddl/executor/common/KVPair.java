/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.taobao.tddl.executor.common;

import com.taobao.tddl.executor.record.CloneableRecord;



/**
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class KVPair implements Comparable, Cloneable {

    private CloneableRecord key;
    private CloneableRecord value;

    public KVPair(){
        key = CloneableRecord.getNewEmptyRecord();
        value = CloneableRecord.getNewEmptyRecord();
    }

    public KVPair(CloneableRecord key, CloneableRecord value){

        if (key != null) {
            this.key = key;
        } else {
            this.key = CloneableRecord.getNewEmptyRecord();
        }
        if (value != null) {
            this.value = value;
        } else {
            this.value = CloneableRecord.getNewEmptyRecord();
        }
    }

    public CloneableRecord getKey() {
        return key;
    }

    public CloneableRecord getValue() {
        return value;
    }

    public void setKey(CloneableRecord key) {
        this.key = key;
    }

    public void setValue(CloneableRecord value) {
        this.value = value;
    }

    @Override
    public Object clone() {
        KVPair o = null;
        try {
            o = (KVPair) super.clone();
            if (key != null) {
                o.key = (CloneableRecord) key.clone();
            }
            if (value != null) {
                o.value = (CloneableRecord) value.clone();
            }
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException("Clone not supported: " + ex.getMessage());
        }
        return o;
    }

    @Override
    public String toString() {
        return "KVPair{" + "key=" + key + ", value=" + value + '}';
    }

    @Override
    public int compareTo(Object o) {
        return key.compareTo(((KVPair) o).key);
    }
}
