package com.taobao.tddl.atom;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.taobao.tddl.atom.common.TAtomConstants;
import com.taobao.tddl.atom.config.TAtomDsConfHandle;
import com.taobao.tddl.atom.config.listener.AtomDbStatusListener;
import com.taobao.tddl.atom.exception.AtomAlreadyInitException;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 动态数据源，支持数据源参数动态修改
 * 
 * @author qihao
 */
public class TAtomDataSource extends AbstractTAtomDataSource {

    protected static Logger                       logger             = LoggerFactory.getLogger(TAtomDataSource.class);

    private static Map<String, TAtomDsConfHandle> cacheConfHandleMap = new HashMap<String, TAtomDsConfHandle>();

    private volatile TAtomDsConfHandle            dsConfHandle       = new TAtomDsConfHandle();

    @Override
    public void init(String appName, String dsKey, String unitName) throws Exception {
        setAppName(appName);
        setDbKey(dsKey);
        setUnitName(unitName);
        init();
    }

    public void init() throws Exception {
        String dbName = TAtomConstants.getDbNameStr(this.getAppName(), this.getDbKey());
        synchronized (cacheConfHandleMap) {
            TAtomDsConfHandle cacheConfHandle = cacheConfHandleMap.get(dbName);
            if (null == cacheConfHandle) {
                // 初始化config的管理器
                this.dsConfHandle.init();
                cacheConfHandleMap.put(dbName, dsConfHandle);
                logger.info("create new TAtomDsConfHandle dbName : " + dbName);
            } else {
                dsConfHandle = cacheConfHandle;
                logger.info("use the cache TAtomDsConfHandle dbName : " + dbName);
            }
        }
    }

    /**
     * 清除掉所有数据源
     */
    public static void cleanAllDataSource() {
        synchronized (cacheConfHandleMap) {
            for (TAtomDsConfHandle handles : cacheConfHandleMap.values()) {
                try {
                    handles.destroyDataSource();
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
            }
            cacheConfHandleMap.clear();
        }
    }

    /**
     * 刷新数据源
     */
    public void flushDataSource() {
        this.dsConfHandle.flushDataSource();
    }

    /**
     * 销毁数据源，慎用
     * 
     * @throws Exception
     */
    public void destroyDataSource() throws Exception {
        String dbName = TAtomConstants.getDbNameStr(this.getAppName(), this.getDbKey());
        synchronized (cacheConfHandleMap) {
            this.dsConfHandle.destroyDataSource();
            cacheConfHandleMap.remove(dbName);
        }
    }

    public String getAppName() {
        return this.dsConfHandle.getAppName();
    }

    public String getDbKey() {
        return this.dsConfHandle.getDbKey();
    }

    public void setAppName(String appName) throws AtomAlreadyInitException {
        this.dsConfHandle.setAppName(TStringUtil.trim(appName));
    }

    public void setDbKey(String dbKey) throws AtomAlreadyInitException {
        this.dsConfHandle.setDbKey(TStringUtil.trim(dbKey));
    }

    public void setUnitName(String unitName) {
        this.dsConfHandle.setUnitName(unitName);
    }

    public TAtomDbStatusEnum getDbStatus() {
        return this.dsConfHandle.getStatus();
    }

    public void setDbStatusListeners(List<AtomDbStatusListener> dbStatusListeners) {
        this.dsConfHandle.setDbStatusListeners(dbStatusListeners);
    }

    public void setSingleInGroup(boolean isSingleInGroup) {
        this.dsConfHandle.setSingleInGroup(isSingleInGroup);
    }

    /** =======以下是设置本地优先的配置属性，如果设置了会忽略推送的配置而使用本地的配置======= */
    public void setPasswd(String passwd) throws AtomAlreadyInitException {
        this.dsConfHandle.setLocalPasswd(passwd);
    }

    public void setDriverClass(String driverClass) throws AtomAlreadyInitException {
        this.dsConfHandle.setLocalDriverClass(driverClass);
    }

    public TAtomDbTypeEnum getDbType() {
        return this.dsConfHandle.getDbType();
    }

    public void setSorterClass(String sorterClass) throws AtomAlreadyInitException {
        this.dsConfHandle.setLocalSorterClass(sorterClass);
    }

    public void setConnectionProperties(Map<String, String> map) throws AtomAlreadyInitException {
        this.dsConfHandle.setLocalConnectionProperties(map);
    }

    protected DataSource getDataSource() throws SQLException {
        return this.dsConfHandle.getDataSource();
    }
}
