package com.taobao.tddl.common.mockdatasource.param;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SetObject2Handler implements ParameterHandler {
	public void setParameter(PreparedStatement stmt, Object[] args)
			throws SQLException {
		stmt.setObject((Integer) args[0], args[1], (Integer) args[2]);
	}
}
