package com.taobao.tddl.common.mockdatasource.param;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SetAsciiStreamHandler implements ParameterHandler {
	public void setParameter(PreparedStatement stmt, Object[] args)
			throws SQLException {
		stmt.setAsciiStream((Integer) args[0], (InputStream) args[1], (Integer) args[2]);
	}
}
