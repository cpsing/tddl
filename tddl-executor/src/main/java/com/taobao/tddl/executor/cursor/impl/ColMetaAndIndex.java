package com.taobao.tddl.executor.cursor.impl;

import com.taobao.tddl.executor.cursor.impl.CursorMetaImp.ColumnHolder;

public class ColMetaAndIndex {

    String       name;
    ColumnHolder columnHolder;

    public String getTable() {
        return columnHolder.tablename;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getIndex() {
        return columnHolder.index;
    }

    public ColumnHolder getColumnHolder() {
        return columnHolder;
    }

    public void setColumnHolder(ColumnHolder columnHolder) {
        this.columnHolder = columnHolder;
    }

}
