package com.taobao.tddl.executor.record;

import java.util.Collections;

import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.common.IRecord;

/**
 * @author mengshi.sunmengshi 2013-11-29 下午2:41:09
 * @since 5.0.0
 */
public abstract class CloneableRecord implements IRecord, Cloneable {

    // public static final CloneableRecord EMPTY_RECORD =
    // CodecFactory.getInstance(CodecFactory.AVRO).getCodec(Collections.EMPTY_LIST).newEmptyRecord();
    public abstract Object get(String name, String key);

    public abstract Object getIngoreTableName(String key);

    public abstract Object getIngoreTableNameUpperCased(String key);

    public abstract CloneableRecord put(String name, String key, Object value);

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException("Clone not supported: " + ex.getMessage());
        }
    }

    public static CloneableRecord getNewEmptyRecord() {
        return CodecFactory.getInstance(CodecFactory.AVRO).getCodec(Collections.EMPTY_LIST).newEmptyRecord();
    }

}
