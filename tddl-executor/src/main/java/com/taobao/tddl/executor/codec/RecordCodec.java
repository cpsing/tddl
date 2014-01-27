package com.taobao.tddl.executor.codec;

import com.taobao.tddl.executor.record.CloneableRecord;

/**
 * @author mengshi.sunmengshi 2013-12-2 下午6:25:22
 * @since 5.0.0
 */
public interface RecordCodec<T> {

    byte[] encode(CloneableRecord record);

    CloneableRecord decode(byte[] bytes);

    CloneableRecord newEmptyRecord();

}
