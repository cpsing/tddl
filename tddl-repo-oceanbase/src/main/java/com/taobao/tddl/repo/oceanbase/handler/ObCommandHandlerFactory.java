package com.taobao.tddl.repo.oceanbase.handler;

import com.taobao.tddl.executor.handler.DeleteHandler;
import com.taobao.tddl.executor.handler.IndexNestedLoopJoinHandler;
import com.taobao.tddl.executor.handler.MergeHandler;
import com.taobao.tddl.executor.handler.NestedLoopJoinHandler;
import com.taobao.tddl.executor.handler.SortMergeJoinHandler;
import com.taobao.tddl.executor.handler.UpdateHandler;
import com.taobao.tddl.repo.mysql.handler.CommandHandlerFactoryMyImp;
import com.taobao.tddl.repo.mysql.handler.InsertMyHandler;
import com.taobao.tddl.repo.mysql.handler.QueryMyHandler;
import com.taobao.tddl.repo.mysql.handler.ReplaceMyHandler;

/**
 * @author dreamond 2014年1月9日 下午5:00:51
 * @since 5.0.0
 */
public class ObCommandHandlerFactory extends CommandHandlerFactoryMyImp {

    public ObCommandHandlerFactory(){
        INSERT_HANDLER = new InsertMyHandler();
        UPDATE_HANDLER = new UpdateHandler();
        DELETE_HANDLER = new DeleteHandler();
        REPLACE_HANDLER = new ReplaceMyHandler();
        QUERY_HANDLER = new QueryMyHandler();
        MERGE_HANDLER = new MergeHandler();
        INDEX_NEST_LOOP_JOIN_HANDLER = new IndexNestedLoopJoinHandler();
        NEST_LOOP_JOIN_HANDLER = new NestedLoopJoinHandler();
        SORT_MERGE_JOIN_HANDLER = new SortMergeJoinHandler();
        CONDENSABLE_JOIN_HANDLER = new QueryMyHandler();
    }
}
