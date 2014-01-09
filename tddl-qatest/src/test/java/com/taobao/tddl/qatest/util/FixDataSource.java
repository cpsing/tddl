package com.taobao.tddl.qatest.util;

import javax.sql.DataSource;

public interface FixDataSource {

	public DataSource getMasterDsByColumn(long column);

	public DataSource getSlaveDsByColumn(long column);

	public DataSource getMasterDsByIndex(int index);

	public DataSource getSlaveDsByIndex(int index);

	public String getTableName(long column, String tablePrefix);

	public int getDbIndex(long column);
}
