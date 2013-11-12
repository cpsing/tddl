package com.taobao.tddl.common.mockdatasource.param;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SetBlobHandler implements ParameterHandler {
	public void setParameter(PreparedStatement stmt, Object[] args)
			throws SQLException {
		stmt.setBlob((Integer) args[0], (Blob) args[1]);
	}
}
