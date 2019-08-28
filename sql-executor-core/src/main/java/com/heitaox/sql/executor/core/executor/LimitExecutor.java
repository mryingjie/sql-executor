package com.heitaox.sql.executor.core.executor;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import joinery.DataFrame;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class LimitExecutor extends BaseExecutor {

    private SQLLimit limit;

    @Override
    public DataFrame execute(DataFrame df) {
        return executeLimit(df, limit);
    }

    private DataFrame<Object> executeLimit(DataFrame<Object> df, SQLLimit limit) {
        SQLExpr offset = limit.getOffset();
        SQLExpr rowCount = limit.getRowCount();
        int offsetNum = 0;
        int rowCountNum = 0;
        if (offset != null) {
            offsetNum = ((SQLIntegerExpr) offset).getNumber().intValue();
        }
        if (rowCount != null) {
            rowCountNum = ((SQLIntegerExpr) rowCount).getNumber().intValue();
            if (rowCountNum < 0) {
                rowCountNum = df.length() - offsetNum;
            }
        }
        if((offsetNum + rowCountNum) > df.length()){
            rowCountNum = df.length() - offsetNum;
        }
        DataFrame<Object> limitDf = new DataFrame<>(df.columns());
        for (int i = 0; i < rowCountNum; i++) {
            List<Object> row = df.row(i + offsetNum);
            try {
                limitDf.append(row);
            } catch (IndexOutOfBoundsException ex) {

                break;
            }
        }
        return limitDf;
    }
}
