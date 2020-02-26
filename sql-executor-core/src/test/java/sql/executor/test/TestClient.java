package sql.executor.test;

import com.heitaox.sql.executor.SQLExecutor;
import com.heitaox.sql.executor.core.util.DataFrameUntil;
import com.heitaox.sql.executor.source.DataSource;
import com.heitaox.sql.executor.source.file.ExcelDataSource;
import joinery.DataFrame;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * created by Yingjie Zheng at 2019-11-06 10:28
 */
public class TestClient {

    public static void main(String[] args) throws IOException, SQLException {

        DataSource excle = new ExcelDataSource("/Users/lsjr3/Documents/docs/迁移/ip.xlsx", new HashMap<String, Class>());
        ExcelDataSource csv = new ExcelDataSource("/Users/lsjr3/Documents/docs/迁移/lejr_common.csv", new HashMap<String, Class>());
        SQLExecutor sqlExecutor = SQLExecutor.sqlExecutorBuilder
                .putDataSource("ip", excle)
                .putDataSource("lejr_common", csv)
                .enableCache()
                .build();
        // DataFrame dataFrame = sqlExecutor.executeQuery("select 应用名,现在的部署IP,现在的部署端口 from lejr_common");
        // DataFrame dataFrame = sqlExecutor.executeQuery("select 应用名,现在的部署IP,'8080' from lejr_common where 部署方式= 'nginx'");
        DataFrame dataFrame = sqlExecutor.executeQuery("select ip from ip");
        sqlExecutor.cache("tmp", dataFrame);
        StringBuilder sb = new StringBuilder();
        for (Object o : dataFrame) {
            List list = (List)o;
            for (int i = 0; i < list.size(); i++) {
                sb.append(list.get(i));
                if(i == list.size()-1){
                    break;
                }
                sb.append(",");
            }
            sb.append("\n");
        }

        System.out.println(sb.toString());


    }

}
