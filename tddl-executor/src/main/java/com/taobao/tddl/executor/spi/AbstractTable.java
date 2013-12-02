package com.taobao.tddl.executor.spi;

import com.taobao.tddl.optimizer.config.table.TableMeta;

/**
 * @author mengshi.sunmengshi 2013-11-27 下午3:54:19
 * @since 5.1.0
 */
public abstract class AbstractTable implements Table {

    TableMeta  schema;

    Repository repo;

    public AbstractTable(TableMeta schema, Repository repo){
        this.schema = schema;
        this.repo = repo;
        // todo:根据shema中二级索引信息，生成trigger.
    }

    @Override
    public TableMeta getSchema() {
        return schema;
    }

}
