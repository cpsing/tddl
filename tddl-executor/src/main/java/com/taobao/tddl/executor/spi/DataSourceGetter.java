package com.taobao.tddl.executor.spi;

import javax.sql.DataSource;

/**
 * 从不同的存储中拿到相应的DataSource
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午3:56:48
 * @since 5.1.0
 */
public interface DataSourceGetter {

    DataSource getDataSource(String group);
}
