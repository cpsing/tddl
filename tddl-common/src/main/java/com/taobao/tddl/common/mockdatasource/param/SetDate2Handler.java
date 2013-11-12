package com.taobao.tddl.common.mockdatasource.param;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;

public class SetDate2Handler implements ParameterHandler {
	public void setParameter(PreparedStatement stmt, Object[] args)
			throws SQLException {
		stmt.setDate((Integer) args[0], (Date) args[1], (Calendar) args[2]);
	}
}
