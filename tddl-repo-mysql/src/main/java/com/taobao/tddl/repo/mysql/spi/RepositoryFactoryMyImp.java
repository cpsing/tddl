package com.taobao.tddl.repo.mysql.spi;

import com.taobao.tddl.executor.spi.IRepositoryFactory;
import com.taobao.tddl.executor.spi.RepositoryConfig;
import com.taobao.tddl.executor.spi.Repository;

public class RepositoryFactoryMyImp implements IRepositoryFactory {

    @Override
    public Repository buildReponsitory(RepositoryConfig conf) {
        My_Repository myRepo = new My_Repository();
        myRepo.init(conf);
        return myRepo;
    }

}
