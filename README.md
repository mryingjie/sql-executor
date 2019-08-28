# sql-executor

[[License]](./LICENSE)

　　sql-executor是一个统一的支持多种数据源类型的sql执行引擎，帮助开发者在项目中使用sql来快速操作文件、非关系型数据库、关系型数据库等。支持不同数据源之间的关联查询，子查询，union查询和插入、更新、删除等常用DML操作。
目前支持的数据源包括：

文件：

- [x] csv文件
- [x] xls文件
- [x] xlsx文件

非关系型数据库：

- [x] elasticsearch
- [x] mongoDB

关系型数据库
- [x] 以mysql5的标准来解析sql的关系型数据库

## 快速开始
　　可以从github上clone到本地，编译，并将其配置到你本地的maven仓库中或直接添加到项目的lib中。  
　　maven方式：
```xml
      <!-- 以下依赖按需引入，如果只需要关系型数据库或者excel文件数据源只需引入sql-executor-core即可
            默认引入了mysql的驱动包，如果要使用oracle或其他关系型数据库请引入相应驱动包   -->
      <!-- sql-executor-elasticsearch以及sql-executor-mongo均依赖sql-executor-core ，无需再次依赖-->
      

     <dependency>
        <groupId>com.heitaox</groupId>
        <artifactId>sql-executor-core</artifactId>
        <version>1.0</version>
     </dependency>
     
     <dependency>
        <groupId>com.heitaox</groupId>
        <artifactId>sql-executor-elasticsearch</artifactId>
        <version>1.0</version>
     </dependency>
     
     <dependency>
        <groupId>com.heitaox</groupId>
        <artifactId>sql-executor-mongo</artifactId>
        <version>1.0</version>
     </dependency>
```
快速开始案例  
　　如果是文件数据源需要先将文件准备好并在首行定义好表头数据,关系型数据库需要先创建好表结构  
1、mysql && excel join query
```java

public class APP {

    public static void main(String[] args) throws Exception {

        //准备数据源
        RDBMSDataSourceProperties dataSourceProperties = new RDBMSDataSourceProperties();
        dataSourceProperties.setUrl("jdbc:mysql://localhost:3306/tests?useUnicode=true&characterEncoding=utf8&autoReconnect=true&failOverReadOnly=false&autoReconnect=true&failOverReadOnly=false&serverTimezone=GMT%2B8");
        dataSourceProperties.setUsername("root");
        dataSourceProperties.setPassword("root");
        dataSourceProperties.setDriverClass("com.mysql.cj.jdbc.Driver");
        dataSourceProperties.setInitialSize(5);
        dataSourceProperties.setTestOnReturn(false);
        dataSourceProperties.setMinEvictableIdleTimeMillis(50000L);

        //文件数据源默认读取进来的数据都是String类型 如果要更改为别的类型，需要定义schema
        Map<String, Class> schema = new HashMap<>();
        schema.put("年龄", int.class);

        SQLExecutor.SQLExecutorBuilder builder = new SQLExecutor.SQLExecutorBuilder();
        SQLExecutor sqlExecutor = builder
                .putDataSource("t_base", new ExcelDataSource(TestExcelDataSourceDemo.class.getClassLoader().getResource("t_base.csv").getPath(), schema))
                .putDataSource("t_score", new StandardSqlDataSource(dataSourceProperties))
                .enableCache()
                .enableFilterBeforeJoin()
                .build();

        //插入数据
        // sqlExecutor.executeInsert("INSERT INTO t_base (姓名,家庭住址,电话,年龄,性别) VALUES ('刘可', '北京西城', '1881881888', '22', '女'), ('李斯', '北京东城', '1991991999', '23', '女'), ('王海', '北京朝阳', '1661661666', '23', '男'), ('陶俊', '北京海淀', '1771771777', '24', '男');");
        // sqlExecutor.executeInsert("INSERT INTO t_score (id,name,subject,fraction) VALUES ('1', '王海', '语文', '86'), ('2', '王海', '数学', '83'), ('3', '王海', '英语', '93'), ('4', '陶俊', '语文', '88'), ('5', '陶俊', '数学', '84'), ('6', '陶俊', '英语', '94'), ('7', '刘可', '语文', '80'), ('8', '刘可', '数学', '86'), ('9', '刘可', '英语', '88'), ('10', '吴宇', '英语', '99'), ('14', '吴宇', '数学', '99'), ('15', '吴宇', '语文', '99')");

        //查看是否插入
        System.out.println(sqlExecutor.executeQuery("select * from t_base"));
        System.out.println(sqlExecutor.executeQuery("select * from t_score"));

        //关联查询
        String sql = "select a.姓名 ,c.电话,b.subject,c.年龄 from \n" +
                " t_base a left join  t_score b \n" +
                " on a.姓名 = b.name left join  t_base c\n" +
                " on c.姓名 = b.name\n" +
                " where a.姓名 in('刘可','陶俊') and  b.subject = '语文'"+
                " order by a.姓名;";
        DataFrame df = sqlExecutor.executeQuery(sql);
        System.out.println(df);
    }
}


```

2、mongoDB && excel join query
```java
public class APP {

    public static void main(String[] args) throws Exception {

        //定义MongoDB数据源的几种方式
        // MongoDataSourceProperties mongoDataSourceProperties = new MongoDataSourceProperties();
        // mongoDataSourceProperties.setDbName("test");
        // mongoDataSourceProperties.setServerAddress(
        //         Arrays.asList(new ServerAddress("localhost",27017))
        // );

        // MongoDataSource mongoDataSource = new MongoDataSource("localhost",27017,"test");
        MongoDataSource mongoDataSource = new MongoDataSource(Arrays.asList(new ServerAddress("localhost",27017)),"test");

        Map<String, Class> schema = new HashMap<>();
        schema.put("年龄", int.class);

        SQLExecutor.SQLExecutorBuilder builder = new SQLExecutor.SQLExecutorBuilder();
        SQLExecutor sqlExecutor = builder
                .putDataSource("t_base",  new ExcelDataSource(TestMongoDatasourceDemo.class.getClassLoader().getResource("t_base.csv").getPath(), schema))
                .putDataSource("t_score", mongoDataSource)
                .enableCache()
                .build();

        //插入数据
        // sqlExecutor.executeInsert("INSERT INTO t_base (姓名,家庭住址,电话,年龄,性别) VALUES ('刘可', '北京西城', '1881881888', '22', '女'), ('李斯', '北京东城', '1991991999', '23', '女'), ('王海', '北京朝阳', '1661661666', '23', '男'), ('陶俊', '北京海淀', '1771771777', '24', '男');");
        // sqlExecutor.executeInsert("INSERT INTO t_score (id,name,subject,fraction) VALUES ('1', '王海', '语文', '86'), ('2', '王海', '数学', '83'), ('3', '王海', '英语', '93'), ('4', '陶俊', '语文', '88'), ('5', '陶俊', '数学', '84'), ('6', '陶俊', '英语', '94'), ('7', '刘可', '语文', '80'), ('8', '刘可', '数学', '86'), ('9', '刘可', '英语', '88'), ('10', '吴宇', '英语', '99'), ('14', '吴宇', '数学', '99'), ('15', '吴宇', '语文', '99')");

        //验证是否插入成功
        System.out.println(sqlExecutor.executeQuery("select * from t_score"));
        System.out.println(sqlExecutor.executeQuery("select * from t_base"));


        String sql = "select a.姓名 ,c.电话,b.subject,c.年龄 from \n" +
                " t_base a left join  t_score b \n" +
                " on a.姓名 = b.name left join  t_base c\n" +
                " on c.姓名 = b.name\n" +
                " where a.姓名 in('刘可','陶俊') and  b.subject = '语文'"+
                " order by a.姓名;";
        DataFrame df = sqlExecutor.executeQuery(sql);

        System.out.println(df);
    }
}
```
3、elasticsearch && excel join query
```java
public class APP {

    public static void main(String[] args) throws Exception {
        Map<String, Class> schema = new HashMap<>();
        schema.put("年龄", int.class);
        
        ElasticsearchDataSource elasticsearchDataSource = new ElasticsearchDataSource(Arrays.asList(
                new HttpHost("localhost", 9200, "http")
        ));
        
        SQLExecutor.SQLExecutorBuilder builder = new SQLExecutor.SQLExecutorBuilder();

        SQLExecutor sqlExecutor = builder
                .putDataSource("t_base",  new ExcelDataSource(TestMongoDatasourceDemo.class.getClassLoader().getResource("t_base.csv").getPath(), schema))
                .putDataSource("t_score", elasticsearchDataSource)
                .enableFilterFirst()
                // .enableFilterBeforeJoin()
                .enableCache()
                .build();

        //插入数据
        // sqlExecutor.executeInsert("INSERT INTO t_base (姓名,家庭住址,电话,年龄,性别) VALUES ('刘可', '北京西城', '1881881888', '22', '女'), ('李斯', '北京东城', '1991991999', '23', '女'), ('王海', '北京朝阳', '1661661666', '23', '男'), ('陶俊', '北京海淀', '1771771777', '24', '男');");
        // sqlExecutor.executeInsert("INSERT INTO t_score (id,name,subject,fraction) VALUES ('1', '王海', '语文', '86'), ('2', '王海', '数学', '83'), ('3', '王海', '英语', '93'), ('4', '陶俊', '语文', '88'), ('5', '陶俊', '数学', '84'), ('6', '陶俊', '英语', '94'), ('7', '刘可', '语文', '80'), ('8', '刘可', '数学', '86'), ('9', '刘可', '英语', '88'), ('10', '吴宇', '英语', '99'), ('14', '吴宇', '数学', '99'), ('15', '吴宇', '语文', '99')");


        System.out.println(sqlExecutor.executeQuery("select * from t_score"));
        System.out.println(sqlExecutor.executeQuery("select * from t_base"));


        String sql = "select a.姓名 ,c.电话,b.subject,c.年龄 from \n" +
                " t_base a left join  t_score b \n" +
                " on a.姓名 = b.name left join  t_base c\n" +
                " on c.姓名 = b.name\n" +
                " where a.姓名 in('刘可','陶俊') and  b.subject = '语文'"+
                " order by a.姓名;";
        DataFrame df = sqlExecutor.executeQuery(sql);

        System.out.println(df);
        elasticsearchDataSource.close();
    }

}
```
详细代码demo see:[sql-executor-test](https://github.com/mryingjie/sql-executor-test)
## 扩展功能

### 1、自定义数据源
　注意将数据转换为Dataframe后一定要调用DataFrameUntil.setColumnTableAlias(df, tableAlias)，为Datafram的每一列设置别名，这个别名如果sql中没有就是表名，如果sql中就是对应的表别名。  
　关系型数据库   
　　see: [RDBMSDataSource](https://github.com/mryingjie/sql-executor/blob/master/src/main/java/com/heitao/sql/executor/source/rdbms/StandardSqlDataSource.java)

```java
   // 一般情况下只需要使用提供的com.heitao.sql.executor.source.rdbms.StandardSqlDataSource即可
   // 你也可以实现com.heitao.sql.executor.source.RDBMSDataSource接口来自定义实现逻辑

    public class xxx implements RDBMSDataSource{

        @Override
        public DataFrame<Object> executeQuery(String sql) {
            return null;
        }

        @Override
        public int executeInsert(String sql) {
            return 0;
        }
    }
```
　非关系型数据库  
　　see: [NoSQLDataSource](https://github.com/mryingjie/sql-executor/blob/master/src/main/java/com/heitao/sql/executor/source/nosql/MongoDataSource.java)
```java
    // 实现com.heitao.sql.executor.source.NoSQLDataSource接口 并在此定义需要的连接信息
    public class xxxx implements NoSQLDataSource {
    
    
            @Override
            public DataFrame queryAll(String tableNme, String tableAlias) {
                return null;
            }
    
            @Override
            public DataFrame queryByPredicate(String table, String tableAlias, List<PredicateEntity<Object>> predicateEntities) {
                return null;
            }
    
            @Override
            public int insert(List<Map<String, Object>> valueList, String tableName) throws IOException {
                return 0;
            }
    
            @Override
            public int update(Map<String, Object> updateItems, List<PredicateEntity<Object>> predicateEntities, String tableName) throws IOException {
                return 0;
            }
    
            @Override
            public int delete(List<PredicateEntity<Object>> predicateEntities, String tableName) throws IOException {
                return 0;
            }
        }
```
　文件类型的数据源  
　　see: [FileDataSource](https://github.com/mryingjie/sql-executor/blob/master/src/main/java/com/heitao/sql/executor/source/file/CsvDataSource.java)
```java
        // 实现com.heitao.sql.executor.source.FileDataSource 并在此定义需要的文件路径等信息
        // 默认读取的数据都是String类型 因此如果要参与数学计算，需要提供对应的schema来描述字段类型
        public class xxx implements FileDataSource {
        
                @Override
                public DataFrame queryAll(String tableNme, String tableAlias) {
                    return null;
                }
        
                @Override
                public int insert(List<Map<String, Object>> valueList, String tableName) throws IOException {
                    return 0;
                }
        
                @Override
                public int update(Map<String, Object> updateItems, List<PredicateEntity<Object>> predicateEntities, String tableName) throws IOException {
                    return 0;
                }
        
                @Override
                public int delete(List<PredicateEntity<Object>> predicateEntities, String tableName) throws IOException {
                    return 0;
                }
            }
```
### 2、自定义函数  
　　工具中目前已经实现的函数包括AVG COUNT MAX MIN SUM IF CONCAT CASE WHEN等可以直接在sql中使用  
　　用户也可以根据自身的需要自定义实现
```java
    //注册函数
    public class APP{
       public static void main(String[] args) {
           SQLExecutor.registFunc("funcName", Func.class);
       }
    }
   
```  
```java
    // 如果要自定义UDF函数 只需继承UDF类泛型为输入和输出的类型，然后实现trans方法并将函数注册到SQLExecutor中即可
    public class xxx extends UDF<String,String>{
    
            @Override
            public String trans(String in) {
                return null;
            }
    }
    
    // 同样的如果要自定义UDAF UDF2 UDF3等函数只需继承对应的类型并实现抽象类的方法然后注册到SQLExecutor中即可
```

### 3、enableCache 
　　如果使用过spark或flink的用户可能有这样的使用场景，sparkSQL中执行一段sql得到的DataFrame需要在下一个sql中使用，这时可以将这个DataFrame创建为一个临时表，然后在这个临时表的基础上执行接下来的sql。同样的我们的SQLExecutor也支持。
```java
public class App {

    public static void main(String[] args) {
        MongoDataSource mongoDataSource = new MongoDataSource(Arrays.asList(new ServerAddress("localhost",27017)),"dbName");

        SQLExecutor sqlExecutor = SQLExecutor.sqlExecutorBuilder
                .putDataSource("Rating", mongoDataSource)
                //开启缓存
                .eableCache()
                .build();
        //或者调用sqlExecutor的方法 来开启缓存
        // sqlExecutor.enableCache();
        
        DataFrame df = sqlExecutor.executeQuery("select sum(score) sum_score ,uid,count(*) from Rating where uid < 20 and mid > 1000 group by uid order by sum_score");
        System.out.println(df);
        sqlExecutor.cache("tmp", df);
        DataFrame dataFrame = sqlExecutor.executeQuery("select * from tmp");
        System.out.println(dataFrame);
        
        //临时表使用完后可以将其从缓存中删除
        sqlExecutor.removeCache("tmp");
    }
}
```

### 4、enableFilterBeforeJoin && enableFilterFirst
　　工作中我们在使用join查询的时候很多情况下都有where子句来筛选结果数据，一般的执行逻辑是先join后再筛选，其实绝大部分情况下可以先进行筛选再join查询，这样可以大大提高join查询的性能，同时搭配enableFilterFirst使用也可以减少与数据源之间的网络IO。 但是要求sql的where语句必须指明字段于哪个表，尽量不要使用十分复杂的过滤条件例如带括号的，以及两个表字段互相之间的比较(a.column1 = b.column2)，以及 or 等， 否则结果将不能得到保证。  
　　目前enableFilterFirst只针对非关系型数据库和关系型数据库，文件类型的数据源并不完全支持，就算开启了这个功能也会执行全表扫描将所有数据加载到内存然后执行先执行筛选再join,也能提升很大的性能。如果有必要，你可以自己实现这个功能。通过实现FileDataSource然后复写。  
　　当使用单表查询时，如果使用的是非关系型数据库，默认会全表扫描，然后在本地进程中对数据进行筛选过滤。当开启enableFilterFirst后，会在数据库层面先过滤筛选然后读取数据，可以减少网络io提高性能，但是当操作elasticsearch的时候因为分词的原因会带来不一样的结果。  
　　需要注意的是当开启enableFilterFirst后将自动开启enableFilterBeforeJoin功能。  
```java
public class App {

    public static void main(String[] args) {
        dataSourceProperties = new RDBMSDataSourceProperties();
                dataSourceProperties.setUrl("jdbc:mysql://localhost:3306/tests?useUnicode=true&characterEncoding=utf8&autoReconnect=true&failOverReadOnly=false&autoReconnect=true&failOverReadOnly=false&serverTimezone=GMT%2B8");
                dataSourceProperties.setUsername("root");
                dataSourceProperties.setPassword("root");
                dataSourceProperties.setDriverClass("com.mysql.cj.jdbc.Driver");
                dataSourceProperties.setInitialSize(5);
                dataSourceProperties.setTestOnReturn(false);
                RDBMSDataSource rdbmsDataSource = new StandardSqlDataSource(dataSourceProperties);
       Map<String, Class> schema = new HashMap<>();
               schema.put("年龄", int.class);
               SQLExecutor sqlExecutor = SQLExecutor.sqlExecutorBuilder
                       .putDataSource("t_base", new CsvDataSource("/Users/lsjr3/Documents/docs/t_base.xlsx", schema))
                       .putDataSource("t_score", rdbmsDataSource)
                       .eableCache()
                       .enableFilterFirst()
                       //开启了enableFilterFirst 将也会开启执行enableFilterBeforeJoin()方法
                       // .enableFilterBeforeJoin()
                       .build();
               String sql = "select a.姓名 ,c.电话,b.subject,c.年龄 from \n" +
                       " t_base a left join  t_score b \n" +
                       " on a.姓名 = b.name left join  t_base c\n" +
                       " on c.姓名 = b.name\n" +
                       " where a.姓名 in('刘可','陶俊') and a.电话 = '1881881888' and b.subject = '语文'"+
                       " order by a.姓名;";
               long l = System.currentTimeMillis();
               DataFrame df = sqlExecutor.executeQuery(sql);
               System.out.println(df);
               System.out.println(System.currentTimeMillis()-l);
    }
}
```
更多使用的demo请参考：[CoreTest](https://github.com/mryingjie/sql-executor/blob/master/src/test/CoreTest.java)

### 需要注意的点
1、sql中的表以及字段尽量都提供别名，否则在关联查询以及子查询中可能出现找不到对应的表的情况。  
2、如果查询的字段有别名having、order by、limit子句中尽量都使用其别名，否则可能出现找不到对应字段的问题。  
3、当使用默认的ElasticsearchDataSource时，如果表字段的类型是String，注意分词，where中的筛选结果因分词的不同可能会出现不同的结果。这里是按照条件中的值
去es中匹配对应的表中字段分词后的结果，匹配到就查询出来。而不是单纯的相等不相等的关系，使用的时候请一定测试结果是否是预期的效果。
例如：name :'张三'  在es中'张三'可能会被分词器分为'张'和'三' 。如果where name = '张' 或 where name = '三' 都会匹配到，但是where name = '张三' 匹配不到。如果在es中'张三'被分词器分为'张'、'三'、'张三'。那么where name = '张三' 就可以匹配到。而且如果执行的是更新删除操作，会有延迟，如果刚执行完更新删除操作就查询可能会查询不到。
### 以后的更新展望
1、SQLExecutor目前并不是线程安全的类，在多线程的情境下请谨慎使用，在以后的更新中会支持多线程下的使用。  
2、目前不支持从HDFS上读取文件，未来会支持。  
3、目前只支持UDF函数和UDAF函数 在以后的更新中会慢慢增加对UDTF函数以及window函数的支持

### 作者的一些话
这个工具是本人第一个也是第一次完整的开源一个项目。第一版从开始到发布历时大概一个月左右，完全是在本人工作之余即兴而做，项目中的所有代码均为本人自己手写和查阅相关api资料所得。由于经验不足，以及自己的能力问题可能会有很多需要改进的地方，和潜在的没有发现的bug。如果哪位大神看到哪里有什么需要改进的地方或bug请联系我，万分感谢！！！  
联系方式：  
微信号：wxid_8sbv70n9eak322  
邮箱：veavalon@163.com
