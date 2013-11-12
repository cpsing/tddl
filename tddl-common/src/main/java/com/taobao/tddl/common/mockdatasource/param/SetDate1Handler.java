package com.taobao.tddl.common.mockdatasource.param;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SetDate1Handler implements ParameterHandler {
	public void setParameter(PreparedStatement stmt, Object[] args)
			throws SQLException {
		stmt.setDate((Integer) args[0], (Date) args[1]);
	}
}
