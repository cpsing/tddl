package com.taobao.tddl.executor.handler;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.function.ExtraFunction;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.IPut.PUT_TYPE;

public class UpdateHandler extends PutHandlerCommon {

    public UpdateHandler(){
        super();
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected int executePut(ExecutionContext executionContext, IPut put, ITable table, IndexMeta meta)
                                                                                                       throws TddlException {

        ITransaction transaction = executionContext.getTransaction();
        int affect_rows = 0;
        IPut update = put;
        ISchematicCursor conditionCursor = null;
        try {
            conditionCursor = ExecutorContext.getContext()
                .getTopologyExecutor()
                .execByExecPlanNode(update.getQueryTree(), executionContext);

            IRowSet kv = null;
            IndexMeta primaryMeta = table.getSchema().getPrimaryIndex();
            CloneableRecord key = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
                .getCodec(primaryMeta.getKeyColumns())
                .newEmptyRecord();
            CloneableRecord value = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
                .getCodec(primaryMeta.getValueColumns())
                .newEmptyRecord();

            while ((kv = conditionCursor.next()) != null) {
                affect_rows++;
                for (ColumnMeta cm : primaryMeta.getValueColumns()) {
                    Object val = getValByColumnMeta(kv, cm);
                    value.put(cm.getName(), val);
                    for (int i = 0; i < update.getUpdateColumns().size(); i++) {
                        String cname = ExecUtils.getColumn(update.getUpdateColumns().get(i)).getColumnName();
                        if (cm.getName().equals(cname)) {
                            Object v = update.getUpdateValues().get(i);
                            if (v instanceof IFunction) {
                                IFunction func = ((IFunction) v);
                                ((ExtraFunction) func.getExtraFunction()).serverMap((IRowSet) null);
                                v = func.getExtraFunction().getResult();
                            }

                            value.put(cname, v);
                        }
                    }
                }
                for (ColumnMeta cm : primaryMeta.getKeyColumns()) {
                    Object val = getValByColumnMeta(kv, cm);
                    key.put(cm.getName(), val);
                    for (int i = 0; i < update.getUpdateColumns().size(); i++) {
                        String cname = (ExecUtils.getColumn(update.getUpdateColumns().get(i))).getColumnName();
                        Object v = update.getUpdateValues().get(i);
                        if (cm.getName().equals(cname)) {
                            key.put(cname, v);
                        }
                    }
                }
                prepare(transaction, table, kv, key, value, PUT_TYPE.UPDATE);
                table.put(executionContext, key, value, meta, put.getTableName());
            }
        } catch (TddlException ex) {
            throw ex;
        } finally {
            if (conditionCursor != null) {
                List<TddlException> exs = new ArrayList();
                exs = conditionCursor.close(exs);
                if (!exs.isEmpty()) {
                    throw GeneralUtil.mergeException(exs);
                }
            }
        }
        return affect_rows;
    }

    private Object getValByColumnMeta(IRowSet kv, ColumnMeta cm) {
        Object val = ExecUtils.getValueByTableAndName(kv, cm.getTableName(), cm.getName());
        return val;
    }

}
