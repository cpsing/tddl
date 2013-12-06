package com.taobao.tddl.repo.mysql.spi;

import com.taobao.tddl.executor.repo.RepositoryConfig;
import com.taobao.tddl.executor.spi.IRepositoryFactory;
import com.taobao.tddl.executor.spi.IRepository;

public class RepositoryFactoryMyImp implements IRepositoryFactory {

    @Override
    public IRepository buildReponsitory(RepositoryConfig conf) {
        My_Repository myRepo = new My_Repository();
        myRepo.init(conf);
        return myRepo;
    }

}
