package com.taobao.tddl.common.mockdatasource.param;

import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SetCharacterStreamHandler implements ParameterHandler {
	public void setParameter(PreparedStatement stmt, Object[] args)
			throws SQLException {
		stmt.setCharacterStream((Integer) args[0], (Reader) args[1], (Integer) args[2]);
	}
}
