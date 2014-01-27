package com.taobao.tddl.executor.cursor;

import java.util.List;

/**
 * @author mengshi.sunmengshi 2013-12-18 下午7:37:20
 * @since 5.0.0
 */
public interface IMergeCursor extends ISchematicCursor {

    public List<ISchematicCursor> getISchematicCursors();
}
