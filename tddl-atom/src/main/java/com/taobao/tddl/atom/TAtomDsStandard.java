package com.taobao.tddl.atom;

import java.io.PrintWriter;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.taobao.tddl.common.model.Atom;

public interface TAtomDsStandard extends DataSource {

    /**
     * @param appName
     * @param dsKey
     */
    public void init(String appName, String dsKey, String unitName) throws Exception;

    public void setLogWriter(PrintWriter out) throws SQLException;

    public void setLoginTimeout(int seconds) throws SQLException;

    public void setShutDownMBean(boolean shutDownMBean);

    public TAtomDbTypeEnum getDbType();

    public TAtomDbStatusEnum getDbStatus();

    public void destroyDataSource() throws Exception;

    public void setAtom(Atom atom);

    public Atom getAtom();

}
