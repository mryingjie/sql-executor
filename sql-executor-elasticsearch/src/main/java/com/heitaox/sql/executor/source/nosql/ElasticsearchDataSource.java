package com.heitaox.sql.executor.source.nosql;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.heitaox.sql.executor.core.entity.PredicateEntity;
import com.heitaox.sql.executor.core.util.DataFrameUntil;
import com.heitaox.sql.executor.source.NoSQLDataSource;
import joinery.DataFrame;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Author ZhengYingjie
 * @Date 2019-08-13
 * @Description
 */
@Slf4j
@SuppressWarnings("all")
public class ElasticsearchDataSource implements NoSQLDataSource {

    private RestHighLevelClient client;

    private static final String updateScriptContant = "ctx._source.";


    public ElasticsearchDataSource(RestHighLevelClient client) {
        this.client = client;
    }

    public ElasticsearchDataSource(List<HttpHost> httpHosts) {
        HttpHost[] httpHostsArray = new HttpHost[httpHosts.size()];
        client = new RestHighLevelClient(RestClient.builder(httpHosts.toArray(httpHostsArray)));
    }


    @Override
    public DataFrame queryAll(String tableNme, String tableAlias) {
        SearchRequest searchRequest = new SearchRequest(tableNme);
        SearchResponse searchResponse = null;
        try {
            log.info("query all data with index [{}] in es",tableNme);
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("Query es to get data failed by IOException", e);
        }
        if (searchResponse == null) {
            return null;
        }
        RestStatus status = searchResponse.status();
        if (!RestStatus.OK.equals(status)) {
            log.error("Query es to get data failed restStatus:{}", status);
            return null;
        }
        int failedShards = searchResponse.getFailedShards();
        if (failedShards != 0) {
            ShardSearchFailure[] shardFailures = searchResponse.getShardFailures();
            log.error("Query es to get data partially failed , shardId:{}", Stream.of(shardFailures).map(x -> x.shard().getShardId()).collect(Collectors.toList()));
            return null;
        }
        //处理搜索命中文档结果
        SearchHits hits = searchResponse.getHits();
        return transToDataFrame(tableAlias, hits);
    }

    private DataFrame<Object> transToDataFrame(String tableAlias, SearchHits hits) {
        SearchHit[] searchHits = hits.getHits();
        DataFrame<Object> df = null;
        List<Object> row = new ArrayList<>();
        Set<String> columns = null;

        for (SearchHit hit : searchHits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap(); // 取成map对象
            if (df == null) {
                columns = sourceAsMap.keySet();
                df = new DataFrame<>(Arrays.asList(columns.toArray()));
            }
            for (String column : columns) {
                Object o = sourceAsMap.get(column);
                row.add(o);
            }
            df.append(row);
            row.clear();
        }
        if (df != null) {
            DataFrameUntil.setColumnTableAlias(df, tableAlias);
        }
        return df == null?new DataFrame<>():df;
    }


    /**
     * 如果是中文注意分词
     * @param table tableNme
     * @param tableAlias tableAlias
     * @param predicateEntities Ascertain condition
     * @return
     */
    @Override
    public DataFrame queryByPredicate(String table, String tableAlias, List<PredicateEntity<Object>> predicateEntities) {
        if (predicateEntities == null || predicateEntities.size() == 0) {
            return queryAll(table, tableAlias);
        }

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = transPredicateToBoolQueryBuilder(predicateEntities);
        sourceBuilder.query(boolQueryBuilder);
        SearchRequest searchRequest = new SearchRequest(table);
        searchRequest.source(sourceBuilder);
        log.info("query dsl:[{}]",boolQueryBuilder.toString());
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("Query es to get data failed by IOException", e);
            return null;
        }
        if (searchResponse == null) {
            return null;
        }
        RestStatus status = searchResponse.status();
        if (!RestStatus.OK.equals(status)) {
            log.error("Query es to get data failed resultStatus:{}", status);
            return null;
        }
        int failedShards = searchResponse.getFailedShards();
        if (failedShards != 0) {
            ShardSearchFailure[] shardFailures = searchResponse.getShardFailures();
            log.error("Query es to get data partially failed , failuresShardId:{}", Stream.of(shardFailures).map(x -> x.shard().getShardId()).collect(Collectors.toList()));
            return null;
        }
        //处理搜索命中文档结果
        SearchHits hits = searchResponse.getHits();
        return transToDataFrame(tableAlias, hits);
    }

    private BoolQueryBuilder transPredicateToBoolQueryBuilder(List<PredicateEntity<Object>> predicateEntities) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        List<QueryBuilder> mustNot = boolQueryBuilder.mustNot();
        List<QueryBuilder> must = boolQueryBuilder.must();

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
                    must.add(QueryBuilders.termsQuery(field, sqlValues.toArray()));
                } else {
                    // in
                    mustNot.add(QueryBuilders.termsQuery(field, sqlValues.toArray()));
                }
            } else {
                //普通筛选器
                SQLBinaryOperator predicateSymbol = entity.getPredicateSymbol();
                if (SQLBinaryOperator.Equality.equals(predicateSymbol)) {
                    // =
                    must.add(QueryBuilders.termQuery(field, sqlValue));
                } else if (SQLBinaryOperator.GreaterThanOrEqual.equals(predicateSymbol)) {
                    // >=
                    must.add(QueryBuilders.rangeQuery(field).gte(sqlValue));
                } else if (SQLBinaryOperator.LessThanOrEqual.equals(predicateSymbol)) {
                    // <=
                    must.add(QueryBuilders.rangeQuery(field).lte(sqlValue));
                } else if (SQLBinaryOperator.NotEqual.equals(predicateSymbol)) {
                    // !=
                    mustNot.add(QueryBuilders.termsQuery(field, sqlValue));
                } else if (SQLBinaryOperator.GreaterThan.equals(predicateSymbol)) {
                    // >
                    must.add(QueryBuilders.rangeQuery(field).gt(sqlValue));
                } else if (SQLBinaryOperator.LessThan.equals(predicateSymbol)) {
                    // <
                    must.add(QueryBuilders.rangeQuery(field).lt(sqlValue));
                } else if (SQLBinaryOperator.IsNot.equals(predicateSymbol)) {
                    // is not null
                    mustNot.add(QueryBuilders.termQuery(field, null));
                } else if (SQLBinaryOperator.Is.equals(predicateSymbol)) {
                    // is null
                    must.add(QueryBuilders.termQuery(field, null));

                } else if (SQLBinaryOperator.Like.equals(predicateSymbol)) {
                    // like
                    String s = sqlValue.toString();

                    if (s.startsWith("%") && s.endsWith("%")) {
                        //包含

                        String substring = s.substring(1, s.length() - 1);
                        must.add(QueryBuilders.regexpQuery(field, "([\\s\\S]*?)" + substring + "([\\s\\S]*?)"));
                    } else if (s.startsWith("%")) {
                        //以...结尾
                        String substring = s.substring(1);
                        must.add(QueryBuilders.regexpQuery(field, "([\\s\\S]*?)" + substring));
                    } else if (s.endsWith("%")) {
                        // 以...开头
                        String substring = s.substring(0, s.length() - 1);
                        must.add(QueryBuilders.regexpQuery(field, substring + "([\\s\\S]*?)"));
                    } else {
                        //相当于 =
                        must.add(QueryBuilders.termQuery(field, sqlValue));
                    }

                }
            }
        }
        return boolQueryBuilder;
    }

    @Override
    public int insert(List<Map<String, Object>> valueList, String tableName) throws IOException {
        BulkRequest request = new BulkRequest();
        for (Map<String, Object> map : valueList) {
            IndexRequest indexRequest = new IndexRequest(tableName);
            indexRequest.source(map);
            request.add(indexRequest);
        }
        int insert = 0;
        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        if (bulkResponse != null) {
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                DocWriteResponse itemResponse = bulkItemResponse.getResponse();
                if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                        || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                    insert++;
                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                    insert++;
                }
            }
        }
        return insert;
    }

    @Override
    public int update(Map<String, Object> updateItems, List<PredicateEntity<Object>> predicateEntities, String tableName) throws IOException {
        if(updateItems == null || updateItems.size()==0 ){
            return 0;
        }
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(tableName);
        updateByQueryRequest.setQuery(transPredicateToBoolQueryBuilder(predicateEntities));
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : updateItems.entrySet()) {
            Object value = entry.getValue();
            if(value instanceof String){
                value = "'" + value.toString() + "'";
            }
            sb.append(updateScriptContant).append(entry.getKey()).append("=").append(value).append(";");
        }
        updateByQueryRequest.setScript(new Script(sb.toString()));
        BulkByScrollResponse bulkByScrollResponse = client.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
        return (int)bulkByScrollResponse.getUpdated();
    }

    @Override
    public int delete(List<PredicateEntity<Object>> predicateEntities, String tableName) throws IOException {

        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(tableName);
        deleteByQueryRequest.setQuery(transPredicateToBoolQueryBuilder(predicateEntities));
        BulkByScrollResponse bulkByScrollResponse = client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
        return (int)bulkByScrollResponse.getUpdated();
    }


    public void close() {
        if (this.client != null) {
            try {
                client.close();
            } catch (IOException e) {
                log.error("RestHighLevelClient close failed", e);
            }
        }
    }

}
