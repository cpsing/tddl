package com.taobao.tddl.optimizer.config.table;

import java.util.Collection;

import com.taobao.tddl.common.model.lifecycle.Lifecycle;

/**
 * 用来描述一个逻辑表由哪些key-val组成的 <br/>
 * 屏蔽掉不同的schema存储，存储可能会是本地,diamond或zk schema
 * 
 * @author jianxing <jianxing.qx@taobao.com>
 * @author whisper
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 */
public interface SchemaManager extends Lifecycle {

    public TableMeta getTable(String tableName);

    public void putTable(String tableName, TableMeta tableMeta);

    public Collection<TableMeta> getAllTables();

}
