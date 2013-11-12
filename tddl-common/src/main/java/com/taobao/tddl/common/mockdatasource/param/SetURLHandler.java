package com.taobao.tddl.common.mockdatasource.param;

import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SetURLHandler implements ParameterHandler {
	public void setParameter(PreparedStatement stmt, Object[] args)
			throws SQLException {
		stmt.setURL((Integer) args[0], (URL) args[1]);
	}
}
