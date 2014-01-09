package com.taobao.tddl.qatest.util;

import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;

public class FixDataSourceImpl implements FixDataSource {
	private int dbCount;
	private int tableCount;
	private int tableLength;
	private List<DataSource> masterDsList;
	private List<DataSource> slaveDsList;

	public DataSource getMasterDsByColumn(long column) {
		int dbIndex = getDbIndex(column);
		return masterDsList.get(dbIndex);
	}

	public DataSource getSlaveDsByColumn(long column) {
		int dbIndex = getDbIndex(column);
		return slaveDsList.get(dbIndex);
	}

	public DataSource getMasterDsByIndex(int index) {
		return masterDsList.get(index);
	}

	public DataSource getSlaveDsByIndex(int index) {
		return slaveDsList.get(index);
	}

	public String getTableName(long userId, String tablePrefix) {
		long tableIndex = userId % tableCount;
		String tableFullName = tablePrefix + "_" + StringUtils.leftPad(tableIndex + "", tableLength, "0");
		return tableFullName;
	}

	public int getDbIndex(long column) {
		return (int) ((column / (tableCount / dbCount)) % dbCount);
	}

	public int getDbCount() {
		return dbCount;
	}

	public void setDbCount(int dbCount) {
		this.dbCount = dbCount;
	}

	public int getTableCount() {
		return tableCount;
	}

	public void setTableCount(int tableCount) {
		this.tableCount = tableCount;
	}

	public int getTableLength() {
		return tableLength;
	}

	public void setTableLength(int tableLength) {
		this.tableLength = tableLength;
	}

	public List<DataSource> getMasterDsList() {
		return masterDsList;
	}

	public void setMasterDsList(List<DataSource> masterDsList) {
		this.masterDsList = masterDsList;
	}

	public List<DataSource> getSlaveDsList() {
		return slaveDsList;
	}

	public void setSlaveDsList(List<DataSource> slaveDsList) {
		this.slaveDsList = slaveDsList;
	}

	public static void main(String[] args) {
		FixDataSourceImpl fixDataSource = new FixDataSourceImpl();
		fixDataSource.setDbCount(16);
		fixDataSource.setTableCount(1024);
		fixDataSource.setTableLength(4);
		System.out.println(fixDataSource.getTableName(1234567890123456789l, "bmw_users_"));
	}

}
