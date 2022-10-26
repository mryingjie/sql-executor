package sql.executor.test;

import com.alibaba.druid.support.json.JSONUtils;
import com.heitaox.sql.executor.SQLExecutor;
import com.heitaox.sql.executor.source.file.ExcelDataSource;
import joinery.DataFrame;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * created by Yingjie Zheng at 2019-11-06 10:28
 */

public class TestClient {


    public static void main(String[] args) throws IOException, SQLException {
        String[] s = {"all_city_source","display_shelf_info_source","non_daily_promotion_sku_source","pre_run_display_source","shop_device_info_source","shop_display_count_source","shop_fruit_display_source","shop_kind_meal_section_status_source","shop_off_sale_sku_source","shop_sku_sale_term_source","week_select_result_source"};
        List<String> testedSource = Arrays.stream(s).collect(Collectors.toList());


        ExcelDataSource excle = new ExcelDataSource("/Users/lejr3/Desktop/三期.xlsx", new HashMap<String, Class>());
        SQLExecutor sqlExecutor = SQLExecutor.sqlExecutorBuilder
                .putDataSource("t1", excle)
                .enableCache()
                .build();
        // DataFrame dataFrame = sqlExecutor.executeQuery("select 应用名,现在的部署IP,现在的部署端口 from lejr_common");
        // DataFrame dataFrame = sqlExecutor.executeQuery("select 应用名,现在的部署IP,'8080' from lejr_common where 部署方式= 'nginx'");
        Map<String,String> sourName2dataNames= new HashMap<>();
        Set<String> allDateNames = new HashSet<>();
        DataFrame dataFrame = sqlExecutor.executeQuery("select * from t1");
        System.out.println(dataFrame);
        for (int i = 0; i < dataFrame.length(); i++) {
            String  sourceName = (String) dataFrame.get(i, 1);
            String dataNames = (String) dataFrame.get(i, 5);
            String replace = dataNames.replace("|", ",").trim();
            String[] split = dataNames.split("\\|");
            allDateNames.addAll(Arrays.asList(split));
            sourName2dataNames.put(sourceName, replace);
        }

        System.out.println(allDateNames.size());
        for (String allDateName : allDateNames) {
            if(!testedSource.contains(allDateName)){
                System.out.println(allDateName);
            }
        }

        // System.out.println(JSONUtils.toJSONString(sourName2dataNames));
    }

}
