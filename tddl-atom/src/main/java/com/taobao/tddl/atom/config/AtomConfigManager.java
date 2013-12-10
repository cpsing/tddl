package com.taobao.tddl.atom.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import com.taobao.tddl.atom.common.TAtomConstants;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.Atom;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataHandlerFactory;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.config.impl.ConfigDataHandlerCity;

/**
 * 全局和应用的配置管理Diamond实现
 * 
 * @author qihao
 */
public class AtomConfigManager implements DbConfManager {

    private static Logger                     logger               = LoggerFactory.getLogger(AtomConfigManager.class);
    private String                            globalConfigDataId;
    private String                            appConfigDataId;
    private String                            unitName;
    private ConfigDataHandlerFactory          configFactory;
    private ConfigDataHandler                 globalHandler;
    private ConfigDataHandler                 appDBHandler;
    private volatile List<ConfigDataListener> globalDbConfListener = new ArrayList<ConfigDataListener>();
    private volatile List<ConfigDataListener> appDbConfListener    = new ArrayList<ConfigDataListener>();
    private Atom                              atom;

    public void init(String appName) {
        Map<String, String> localValues = null;
        if (this.atom != null) {
            localValues = atom.getProperties();
        }
        configFactory = ConfigDataHandlerCity.getFactory(appName, unitName, localValues);

        Map<String, Object> config = new HashMap<String, Object>();
        config.put("group", TAtomConstants.DEFAULT_DIAMOND_GROUP);

        globalHandler = configFactory.getConfigDataHandler(globalConfigDataId,
            globalDbConfListener,
            Executors.newSingleThreadScheduledExecutor(),
            config);
        appDBHandler = configFactory.getConfigDataHandler(appConfigDataId,
            appDbConfListener,
            Executors.newSingleThreadScheduledExecutor(),
            config);
    }

    public String getAppDbConfDataId() {
        return appConfigDataId;
    }

    public String getAppDbDbConf() {
        if (null != appDBHandler) {
            return appDBHandler.getData(ConfigDataHandler.GET_DATA_TIMEOUT,
                ConfigDataHandler.FIRST_CACHE_THEN_SERVER_STRATEGY);
        }
        logger.error("[getDataError] appDBConfig not init !");
        return null;
    }

    public String getGlobalDbConf() {
        if (null != globalHandler) {
            return globalHandler.getData(ConfigDataHandler.GET_DATA_TIMEOUT,
                ConfigDataHandler.FIRST_CACHE_THEN_SERVER_STRATEGY);
        }
        logger.error("[getDataError] globalConfig not init !");
        return null;
    }

    public void setGlobalConfigDataId(String globalConfigDataId) {
        this.globalConfigDataId = globalConfigDataId;
    }

    public String getAppConfigDataId() {
        return appConfigDataId;
    }

    public void setAppConfigDataId(String appConfigDataId) {
        this.appConfigDataId = appConfigDataId;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    /**
     * @param Listener
     */
    public void registerGlobaDbConfListener(ConfigDataListener listener) {
        globalDbConfListener.add(listener);
    }

    /**
     * @param Listener
     */
    public void registerAppDbConfListener(ConfigDataListener listener) {
        appDbConfListener.add(listener);
    }

    @Override
    public void stopDbConfManager() throws TddlException {
        if (null != this.globalHandler) {
            this.globalHandler.destory();
        }
        if (null != this.appDBHandler) {
            this.appDBHandler.destory();
        }
    }

    public void setAtom(Atom atom) {
        this.atom = atom;

    }
}
