package com.taobao.tddl.atom.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import com.taobao.tddl.atom.common.TAtomConstants;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataHandlerFactory;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.config.impl.ConfigDataHandlerCity;

/**
 * 密码管理器Diamond实现
 * 
 * @author qihao
 */
public class DiamondDbPasswdManager implements DbPasswdManager {

    private static Logger                     logger             = LoggerFactory.getLogger(DiamondDbPasswdManager.class);
    private String                            passwdConfDataId;
    private String                            unitName;
    private ConfigDataHandlerFactory          configFactory;
    private ConfigDataHandler                 passwdHandler;
    private volatile List<ConfigDataListener> passwdConfListener = new ArrayList<ConfigDataListener>();

    public void init(String appName) {
        configFactory = ConfigDataHandlerCity.getFactory(appName, unitName);
        Map<String, Object> config = new HashMap<String, Object>();
        config.put("group", TAtomConstants.DEFAULT_DIAMOND_GROUP);
        passwdHandler = configFactory.getConfigDataHandler(passwdConfDataId,
            passwdConfListener,
            Executors.newSingleThreadScheduledExecutor(),
            config);
    }

    public String getPasswd() {
        if (null != passwdHandler) {
            String passwdStr = passwdHandler.getData(ConfigDataHandler.GET_DATA_TIMEOUT,
                ConfigDataHandler.FIRST_CACHE_THEN_SERVER_STRATEGY);
            if (passwdStr == null) {
                logger.error("[getDataError] remote password string is empty !");
                return null;
            }
            return TAtomConfParser.parserPasswd(passwdStr);
        }
        logger.error("[getDataError] passwdConfig not init !");
        return null;
    }

    public void registerPasswdConfListener(ConfigDataListener Listener) {
        passwdConfListener.add(Listener);
    }

    public void setPasswdConfDataId(String passwdConfDataId) {
        this.passwdConfDataId = passwdConfDataId;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public void stopDbPasswdManager() {
        if (null != this.passwdHandler) {
            this.passwdHandler.closeUnderManager();
        }
    }
}
