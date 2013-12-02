package com.taobao.tddl.executor.cursor;

import java.util.List;

public interface IMergeCursor extends ISchematicCursor {

    public List<ISchematicCursor> getISchematicCursors();
}
