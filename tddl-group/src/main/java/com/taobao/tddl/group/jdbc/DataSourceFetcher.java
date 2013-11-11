package com.taobao.tddl.group.jdbc;

import javax.sql.DataSource;

import com.taobao.tddl.common.model.DBType;

/**
 * 为了避免对TGroupDataSource这一层对spring的依赖
 * 
 * @author linxuan
 */
public interface DataSourceFetcher {

    DataSource getDataSource(String key);

    DBType getDataSourceDBType(String key);
}
