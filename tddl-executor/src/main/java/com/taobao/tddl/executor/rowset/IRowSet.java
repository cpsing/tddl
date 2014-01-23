package com.taobao.tddl.executor.rowset;

import com.taobao.tddl.common.model.BaseRowSet;
import com.taobao.tddl.executor.cursor.ICursorMeta;

/**
 * 数据的核心接口，类似ResultSet一样的接口。下面有可能有join的实现，也可能有普通query或Merge实现。
 * 
 * @author Whisper
 */
public interface IRowSet extends BaseRowSet {

    ICursorMeta getParentCursorMeta();
}
