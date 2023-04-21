package edu.northeastern.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

@Service
public class ElasticsearchService {

    private static final String hostname = "localhost";
    private static final Integer elastic_port = 9200;
    private static final String scheme = "http";
    private static final String indexName="indexplan";

    private static final String plan_pcs = "planCostShares";
    private static final String plan_ls = "linkedService";
    private static final String plan_pscs = "planserviceCostShares";
    private static final String plan_lps = "linkedPlanServices";
    private static final String plan_pt = "planType";
    private static final String plan_cd = "creationDate";
    private static final String plan_dd = "deductible";
    private static final String plan_cp = "copay";
    private static final String plan_name = "name";
    private static final String plan_objt = "objectType";
    private static final String plan_objid = "objectId";
    private static final String plan_org = "_org";

    private static final RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, elastic_port, scheme)));
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchService.class);

    /**
     1. Generate or re-generate index in elasticsearch
     **/
    public static void addMessageQueueListener(){
        try {
            GetIndexRequest request = new GetIndexRequest(indexName);
            boolean exist = client.indices().exists(request, RequestOptions.DEFAULT);
            if (exist) {
                client.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
                String index = createElasticIndex();
                logger.info("Index "+index+" successfully destroyed and recreated.");
            } else {
                String index = createElasticIndex();
                logger.info("Index "+index+" successfully created.");
            }
        } catch (IOException ex) {
            logger.error("Error occurred when initializing message queue listener! "+ex.getMessage());
        }
    }

    private static final String indexShards = "index.number_of_shards";
    private static final Integer numOfShards = 3;
    private static final String indexReplicas = "index.number_of_replicas";
    private static final Integer numOfReplicas = 2;

    /**
     1.1 Building index with : mapping(w/ parent-child relationship), shard, replica
     **/
    private static String createElasticIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(Settings.builder()
                .put(indexShards, numOfShards)
                .put(indexReplicas, numOfReplicas));
        DefaultResourceLoader resourceLoader = new DefaultResourceLoader();
        Resource resource = resourceLoader.getResource("classpath:./mapping.json");
        String mappingJson = new String(FileCopyUtils.copyToByteArray(resource.getInputStream()));
        request.mapping(mappingJson, XContentType.JSON);
        return client.indices().create(request, RequestOptions.DEFAULT).index();
    }

    /**
     2 postDocument (recursive)
     **/
    public static String postDocument(JsonNode jsonNode, String parentId, String ancestorId, String name) {
        if(jsonNode==null) return null;
        try{
            IndexRequest request = new IndexRequest(indexName);
            String documentId = jsonNode.get(plan_objid).asText();
            request.id(documentId);
            if(ancestorId == null) {
                ancestorId = parentId;
            }
            if(ancestorId == null) {
                ancestorId = documentId;
            }
            request.routing(ancestorId);
            //1. generate builder
            request.source(generateBuilder(jsonNode, parentId, name));
            //2. building current document
            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
            //3. recursive posting documents
            checkForNestedObjectsInJsonNode(jsonNode, documentId, ancestorId);

            logger.info("Document with id: "+indexResponse.getId()+" posted.");
            return indexResponse.getResult().name();
        }catch (IOException ex){
            logger.error("Error occurred in creating document"+jsonNode.get(plan_objid).asText()+": "+ex.getMessage());
            ex.printStackTrace();
            return null;
        }
    }

    /**
     2.3 for any key-value pairs: if the value is object or array, go recursive and call postDocument
     **/
    private static void checkForNestedObjectsInJsonNode(JsonNode jsonNode, String documentId, String ancestorId) {
        jsonNode.fields().forEachRemaining(e -> {
            String key = e.getKey();
            switch (key){
                case plan_pcs:
                    postDocument(jsonNode.get(plan_pcs), documentId, ancestorId, plan_pcs);
                    break;
                case plan_ls:
                    postDocument(jsonNode.get(plan_ls), documentId, ancestorId, plan_ls);
                    break;
                case plan_pscs:
                    postDocument(jsonNode.get(plan_pscs), documentId, ancestorId, plan_pscs);
                    break;
                case plan_lps:
                    ArrayNode jsonArray = (ArrayNode) jsonNode.get(plan_lps);
                    jsonArray.forEach(jn -> postDocument(jn, documentId, ancestorId, plan_lps));
                    break;
                default:
                    logger.info("Skipping plain text key value pair ("+key+"-"+e.getValue()+") in iterating nested objects.");
                    break;
            }
        });
    }

    private static XContentBuilder generateBuilder(JsonNode jsonNode, String parentId, String name) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            for(String key : new ArrayList<>(Arrays.asList(plan_org, plan_objt, plan_objid, plan_pt, plan_cd, plan_cp, plan_dd, plan_name))){
                if(jsonNode.get(key)!=null){
                    builder.field(key, jsonNode.get(key).asText());
                }
            }
            builder.startObject("plan_join");
            if(parentId==null){
                builder.field("name", "plan");
            }else {
                builder.field("parent", parentId);
                builder.field("name", name);
            }
            builder.endObject();
            builder.endObject();
            return builder;
        }catch (IOException ex){
            logger.error("Error occurs when attempting to building XContentBuilder: "+ex.getMessage());
            return null;
        }
    }

    /**
     3 deleteDocument: query for _routing to delete by documentId
     **/
    public static String deleteDocument(String documentId) throws IOException {
        if(documentId==null)return null;
        try {
            DeleteByQueryRequest request = new DeleteByQueryRequest(indexName);
            QueryBuilder queryBuilder = QueryBuilders.matchQuery("_routing", documentId);
            request.setQuery(queryBuilder);
            request.setRefresh(true);
            BulkByScrollResponse response = client.deleteByQuery(request, RequestOptions.DEFAULT);
            logger.info(response.getDeleted()+" Document(s) with id: "+documentId+" and its children deleted.");
            return response.toString();
        }catch (IOException ex){
            logger.error("Error occurred in creating document "+documentId+". "+ex.getMessage());
            ex.printStackTrace();
            return null;
        }
    }
}
