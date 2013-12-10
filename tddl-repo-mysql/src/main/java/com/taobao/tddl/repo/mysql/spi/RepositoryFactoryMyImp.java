package com.taobao.tddl.repo.mysql.spi;

import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.IRepositoryFactory;

public class RepositoryFactoryMyImp implements IRepositoryFactory {

    @Override
    public IRepository buildRepository() {
        My_Repository myRepo = new My_Repository();
        myRepo.init();
        return myRepo;
    }

}
