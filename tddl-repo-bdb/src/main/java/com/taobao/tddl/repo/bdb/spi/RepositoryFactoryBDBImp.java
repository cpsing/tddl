package com.taobao.tddl.repo.bdb.spi;

import java.util.Map;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.extension.Activate;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.IRepositoryFactory;

@Activate(name = "BDB_JE")
public class RepositoryFactoryBDBImp implements IRepositoryFactory {

    private final static Logger logger = LoggerFactory.getLogger(RepositoryFactoryBDBImp.class);

    @Override
    public IRepository buildRepository(Map properties) {
        String repoConfigFile = GeneralUtil.getExtraCmd(properties, BDBConfig.BDB_REPO_CONFIG_FILE_PATH);
        BDBConfig config = null;

        if (repoConfigFile == null) {
            config = new BDBConfig();
            logger.warn("bdb repo config file is not assigned, use default config");

        } else {
            try {
                config = new BDBConfig(repoConfigFile);
            } catch (Exception e) {
                throw new TddlRuntimeException("bdb repository init error", e);
            }
        }
        JE_Repository jeRepository = null;
        if (config.isHA()) {
            jeRepository = new JE_HA_Repository();

        } else {
            jeRepository = new JE_Repository();

        }
        jeRepository.setConfig(config);
        jeRepository.init();
        return jeRepository;

    }
}
