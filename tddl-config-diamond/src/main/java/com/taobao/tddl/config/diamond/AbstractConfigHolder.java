package com.taobao.tddl.config.diamond;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.diamond.client.BatchHttpResult;
import com.taobao.diamond.client.impl.DiamondEnv;
import com.taobao.diamond.client.impl.DiamondEnvRepo;
import com.taobao.diamond.client.impl.DiamondUnitSite;
import com.taobao.diamond.domain.ConfigInfoEx;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.config.ConfigDataHolder;

public abstract class AbstractConfigHolder implements ConfigDataHolder {

    private static final Logger    log                 = LoggerFactory.getLogger(AbstractConfigHolder.class);

    protected AbstractConfigHolder sonConfigDataHolder = null;

    protected Map<String, String>  configHouse         = new HashMap<String, String>();

    public abstract void init();

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected Map<String, String> queryAndHold(List<String> dataIds, String unitName) {
        DiamondEnv env = null;
        if (!TStringUtil.isEmpty(unitName)) {
            env = DiamondUnitSite.getDiamondUnitEnv(unitName);
        } else {
            env = DiamondEnvRepo.defaultEnv;
        }

        BatchHttpResult queryResult = env.batchQuery(dataIds, "DEFAULT_GROUP", 30000);
        Map<String, String> result = new HashMap<String, String>();
        if (queryResult.isSuccess()) {
            List<ConfigInfoEx> configs = queryResult.getResult();
            for (ConfigInfoEx config : configs) {
                result.put(config.getDataId(), config.getContent());
            }
            addDatas(result);
        } else {
            log.warn("Batch Query Failed. Status Code is " + queryResult.getStatusCode());
        }
        return result;
    }

    protected void addDatas(Map<String, String> confMap) {
        configHouse.putAll(confMap);
    }

    @Override
    public Map<String, String> getData(List<String> dataIds) {
        Map<String, String> result = new HashMap<String, String>();
        for (String dataId : dataIds) {
            result.put(dataId, getData(dataId));
        }
        return result;
    }

    @Override
    public String getData(String dataId) {
        return configHouse.containsKey(dataId) ? configHouse.get(dataId) : getDataFromSonHolder(dataId);
    }

    protected String getDataFromSonHolder(String dataId) {
        return sonConfigDataHolder == null ? null : sonConfigDataHolder.getData(dataId);
    }
}
