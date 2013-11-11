package com.taobao.tddl.group.listener;

import java.util.Map;

import javax.sql.DataSource;

public interface DataSourceChangeListener {

    public void onDataSourceChanged(Map<String, DataSource> dataSources);
}
