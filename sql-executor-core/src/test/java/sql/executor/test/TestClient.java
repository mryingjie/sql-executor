package sql.executor.test;

import com.heitaox.sql.executor.SQLExecutor;
import com.heitaox.sql.executor.core.util.DataFrameUntil;
import com.heitaox.sql.executor.source.DataSource;
import com.heitaox.sql.executor.source.file.ExcelDataSource;
import joinery.DataFrame;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * created by Yingjie Zheng at 2019-11-06 10:28
 */
public class TestClient {

    public static void main(String[] args) throws IOException, SQLException {

        //准备两个文件数据
        DataSource excle = new ExcelDataSource("/Users/Documents/docs/test_table1.xlsx", new HashMap<String, Class>());
        DataSource csv = new ExcelDataSource("/Users/Documents/docs/test_table2.csv", new HashMap<String, Class>());

        //创建sqlExecutor执行引擎
        SQLExecutor sqlExecutor = SQLExecutor.sqlExecutorBuilder
                .putDataSource("test_table1", excle)
                .putDataSource("test_table2", csv)
                .enableCache()
                .build();
        //从table1中查询数据获得DataFrame
        DataFrame dataFrame = sqlExecutor.executeQuery("select 应用名,现在的部署IP,现在的部署端口 from test_table1");

        //将dataFrame转化为插入语句
        String sql = DataFrameUntil.toInsertSql(dataFrame, "test_table2");

        //执行sql  可以在csv文件上查看插入结果
        sqlExecutor.executeInsert(sql);
    }

}
