package com.heitaox.sql.executor.source.nosql;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.heitaox.sql.executor.core.entity.PredicateEntity;
import com.heitaox.sql.executor.core.util.DataFrameUntil;
import com.heitaox.sql.executor.source.NoSQLDataSource;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import joinery.DataFrame;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * @Author ZhengYingjie
 * @Date 2019-08-21
 * @Description
 */
@Slf4j
public class MongoDataSource implements NoSQLDataSource {

    private MongoClient mongoClient;

    private final String DB_NAME;

    private final MongoDatabase database;

    public MongoDataSource(MongoDataSourceProperties properties) {
        MongoCredential credential = MongoCredential.createCredential(properties.getUser(), properties.getDbName(), properties.getPassword());

        MongoClientSettings settings = MongoClientSettings.builder()
                .credential(credential)
                .applyToSslSettings(builder -> builder.enabled(true))
                .applyToClusterSettings(builder ->
                        builder.hosts(properties.getServerAddress()))
                .build();
        mongoClient = MongoClients.create(settings);

        this.DB_NAME = properties.getDbName();
        database = mongoClient.getDatabase(DB_NAME);
    }


    public MongoDataSource(String host, Integer port, String dbName) {
        mongoClient = MongoClients.create("mongodb://" + host + ":" + port);
        this.DB_NAME = dbName;
        database = mongoClient.getDatabase(DB_NAME);
    }


    public MongoDataSource(List<ServerAddress> serverAddreses, String dbName) {
        mongoClient = MongoClients.create(
                MongoClientSettings.builder()
                        .applyToClusterSettings(builder ->
                                builder.hosts(serverAddreses))
                        .build());
        this.DB_NAME = dbName;
        database = mongoClient.getDatabase(DB_NAME);

    }

    public MongoDataSource(MongoClient mongoClient, String dbName) {
        this.mongoClient = mongoClient;
        this.DB_NAME = dbName;
        database = mongoClient.getDatabase(DB_NAME);
    }


    @Override
    public DataFrame queryAll(String tableNme, String tableAlias) {
        MongoCollection<Document> collection = database.getCollection(tableNme);
        FindIterable<Document> documents = collection.find();

        return transToDataFrame(documents, tableAlias);

    }

    private DataFrame<Object> transToDataFrame(FindIterable<Document> documents, String tableAlias) {
        if (documents == null) {
            return null;
        }
        DataFrame<Object> df = null;
        Set<String> columns = null;
        List<Object> row = new ArrayList<>();

        for (Document next : documents) {
            if (df == null) {
                columns = next.keySet();
                columns.remove("_id");
                columns.remove("timestamp");
                df = new DataFrame<>(Arrays.asList(columns.toArray()));
            }
            for (String column : columns) {
                Object o = next.get(column);
                row.add(o);
            }
            df.append(row);
            row.clear();
        }
        if (df == null) {
            return new DataFrame<>();
        }
        DataFrameUntil.setColumnTableAlias(df, tableAlias);

        return df;
    }

    @Override
    public DataFrame queryByPredicate(String table, String tableAlias, List<PredicateEntity<Object>> predicateEntities) {
        if (predicateEntities == null || predicateEntities.size() == 0) {
            return queryAll(table, tableAlias);
        }
        Document document = transPredicateToDocument(predicateEntities);
        MongoCollection<Document> collection = database.getCollection(table);
        FindIterable<Document> documents = collection.find(document == null ? new Document() : document);
        return transToDataFrame(documents, tableAlias);
    }

    private Document transPredicateToDocument(List<PredicateEntity<Object>> predicateEntities) {
        if (predicateEntities == null) {
            return new Document();
        }
        Document document = null;
        for (int i = predicateEntities.size() - 1; i >= 0; i--) {
            PredicateEntity<Object> entity = predicateEntities.get(i);
            // 判断是不是括号
            if (entity.getBrackets() != null) {
                continue;
            }
            //判断是不是and 和 or
            if (entity.getConnecSymbol() != null) {
                if (SQLBinaryOperator.BooleanAnd.equals(entity.getConnecSymbol())) {
                    continue;
                } else {
                    throw new RuntimeException("MongoDataSource does not currently support the (SQLBinaryOperator-OR) syntax in the where clause.");
                }
            }
            String field = entity.getField();
            Object sqlValue = entity.getValue();

            //首先判断是不是in筛选器
            if (sqlValue instanceof List) {
                List sqlValues = (List) sqlValue;
                Boolean not = entity.getNot();
                if (not) {
                    // not in
                    document = document == null ? new Document(field, new Document("$in", sqlValues)) : document.append(field, new Document("$in", sqlValues));
                } else {
                    // in
                    document = document == null ? new Document(field, new Document("$in", sqlValues)) : document.append(field, new Document("$in", sqlValues));

                }
            } else {
                //普通筛选器
                SQLBinaryOperator predicateSymbol = entity.getPredicateSymbol();
                if (SQLBinaryOperator.Equality.equals(predicateSymbol)) {
                    // =
                    document = document == null ? new Document(field, sqlValue) : document.append(field, sqlValue);
                } else if (SQLBinaryOperator.GreaterThanOrEqual.equals(predicateSymbol)) {
                    // >=
                    document = document == null ? new Document(field, new Document("$gte", sqlValue)) : document.append(field, new Document("$gte", sqlValue));
                } else if (SQLBinaryOperator.LessThanOrEqual.equals(predicateSymbol)) {
                    // <=
                    document = document == null ? new Document(field, new Document("$lte", sqlValue)) : document.append(field, new Document("$lte", sqlValue));

                } else if (SQLBinaryOperator.NotEqual.equals(predicateSymbol)) {
                    // !=
                    document = document == null ? new Document(field, new Document("$ne", sqlValue)) : document.append(field, new Document("$ne", sqlValue));
                } else if (SQLBinaryOperator.GreaterThan.equals(predicateSymbol)) {
                    // >
                    document = document == null ? new Document(field, new Document("$gt", sqlValue)) : document.append(field, new Document("$gt", sqlValue));
                } else if (SQLBinaryOperator.LessThan.equals(predicateSymbol)) {
                    // <
                    document = document == null ? new Document(field, new Document("$lt", sqlValue)) : document.append(field, new Document("$lt", sqlValue));
                } else if (SQLBinaryOperator.IsNot.equals(predicateSymbol)) {
                    // is not null
                    document = document == null ? new Document(field, new Document("$ne", null)) : document.append(field, new Document("$ne", null));
                } else if (SQLBinaryOperator.Is.equals(predicateSymbol)) {
                    // is null
                    document = document == null ? new Document(field, new Document("$eq", null)) : document.append(field, new Document("$eq", null));

                } else if (SQLBinaryOperator.Like.equals(predicateSymbol)) {
                    // like
                    String s = sqlValue.toString();

                    if (s.startsWith("%") && s.endsWith("%")) {
                        //包含
                        String substring = s.substring(1, s.length() - 1);
                        document = document == null ? new Document(field, new Document("$regex", "([\\s\\S]*?)" + substring + "([\\s\\S]*?)")) : document.append(field, new Document("$regex", "/" + substring + "/"));

                    } else if (s.startsWith("%")) {
                        //以...结尾
                        String substring = s.substring(1);
                        document = document == null ? new Document(field, new Document("$regex", "([\\s\\S]*?)" + substring)) : document.append(field, new Document("$regex", "/" + substring + "$/"));

                    } else if (s.endsWith("%")) {
                        // 以...开头
                        String substring = s.substring(0, s.length() - 1);
                        document = document == null ? new Document(field, new Document("$regex", substring + "([\\s\\S]*?)")) : document.append(field, new Document("$regex", "/^" + substring + "/"));
                    } else {
                        //相当于 =
                        document = document == null ? new Document(field, sqlValue) : document.append(field, sqlValue);
                    }

                }
            }
        }
        return document;
    }

    @Override
    public int insert(List<Map<String, Object>> valueList, String tableName) {
        if (valueList == null || valueList.size() == 0) {
            return 0;
        }
        MongoCollection<Document> collection = database.getCollection(tableName);
        List<Document> documents = valueList.stream()
                .map(Document::new)
                .collect(Collectors.toList());
        collection.insertMany(documents);

        return valueList.size();
    }

    @Override
    public int update(Map<String, Object> updateItems, List<PredicateEntity<Object>> predicateEntities, String tableName) throws IOException {
        if (updateItems == null || updateItems.size() == 0) {
            return 0;
        }
        Document predicateDoc = transPredicateToDocument(predicateEntities);
        Document itemDoc = new Document(updateItems);

        Document setDoc = new Document("$set", itemDoc);
        MongoCollection<Document> collection = database.getCollection(tableName);
        UpdateResult updateResult = collection.updateMany(predicateDoc == null ? new Document() : predicateDoc, setDoc);
        return (int) updateResult.getModifiedCount();
    }

    @Override
    public int delete(List<PredicateEntity<Object>> predicateEntities, String tableName) {
        Document document = transPredicateToDocument(predicateEntities);
        MongoCollection<Document> collection = database.getCollection(tableName);
        DeleteResult deleteResult = collection.deleteMany(document);
        return (int)deleteResult.getDeletedCount();
    }

    public void closeMongoClient() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }


}
