package com.taobao.tddl.repo.oceanbase.spi;

import javax.sql.DataSource;

import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.repo.mysql.spi.My_Table;

public class Ob_Table extends My_Table {

    public Ob_Table(DataSource ds, TableMeta schema, String groupNodeName){
        super(ds, schema, groupNodeName);
    }

}
