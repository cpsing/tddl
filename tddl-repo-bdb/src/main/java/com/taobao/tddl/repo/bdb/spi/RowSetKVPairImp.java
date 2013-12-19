package com.taobao.tddl.repo.bdb.spi;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.AbstractRowSet;
import com.taobao.tddl.executor.rowset.IRowSet;

public class RowSetKVPairImp extends AbstractRowSet implements IRowSet {

    private final KVPair kvPair;
    private final int    keyLength;

    public RowSetKVPairImp(ICursorMeta cursorMeta, KVPair kvPair){
        super(cursorMeta);
        this.kvPair = kvPair;
        keyLength = kvPair.getKey().getColumnMap().size();
    }

    @Override
    public Object getObject(int index) {
        if (keyLength > index) {
            return kvPair.getKey().getValueByIndex(index);
        } else {
            index = index - keyLength;
            return kvPair.getValue().getValueByIndex(index);
        }
    }

    @Override
    public void setObject(int index, Object value) {
        if (keyLength > index) {
            kvPair.getKey().setValueByIndex(index, value);
        } else {
            index = index - keyLength;
            kvPair.getValue().setValueByIndex(index, value);
        }
    }

    @Override
    public List<Object> getValues() {
        CloneableRecord cr = kvPair.getKey();
        List<Object> ret = null;
        if (cr != null) {
            ret = new ArrayList<Object>(kvPair.getKey().getValueList());
        } else {
            ret = new ArrayList<Object>();
        }
        cr = kvPair.getValue();
        if (cr != null) {
            ret.addAll(cr.getValueList());
        }
        return ret;
    }

}
