package edu.northeastern.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.northeastern.Demo1Application;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;
import redis.clients.jedis.JedisPooled;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Component
public class MessageQueueListener {

    private static final String hostname = "localhost";
    private static final Integer redis_port = 6379;
    private static final Integer elastic_port = 9200;
    private static final String scheme = "http";
    private static final String indexName="indexplan";

    private static final Logger logger = LoggerFactory.getLogger(MessageQueueListener.class);
    private static final JedisPooled jedis = new JedisPooled(hostname, redis_port);
    private static RestHighLevelClient client ;

    private static final String messageQueue = "MessageQueue";
    private static final String workingQueue = "WorkingQueue";
    private static final String messageField = "message";
    private static final String operationField = "operation";
    private static final String operationPost = "post";
    private static final String operationDelete = "delete";

    private static final String indexShards = "index.number_of_shards";
    private static final Integer numOfShards = 3;
    private static final String indexReplicas = "index.number_of_replicas";
    private static final Integer numOfReplicas = 2;

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

    public static void initMessageQueue() {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, elastic_port, scheme)));
        logger.info("Message Queue started at: "+hostname+":"+elastic_port);
        try {
            if (!indexExists()) {
                String index = createElasticIndex();
                logger.info("Index "+index+" created.");
            } else {
                logger.info("Index already existed.");
            }
        } catch (IOException ex) {
            logger.error("Error occurred! "+ex.getMessage());
            return;
        }

        while (true) {
            try {
                Object jobMessage = jedis.rpoplpush(messageQueue, workingQueue);
                if (jobMessage == null) continue;

                JsonNode job = new ObjectMapper().readTree((String)jobMessage);
                String message = job.get(messageField).asText();
                String operation = job.get(operationField).asText();
                logger.info("New Job Message Queue received: Operation: " + operation + ". Message:" + message);
                if (operation.equals(operationPost)) {
                    JsonNode plan = new ObjectMapper().readTree(message);
                    String result = postDocument(plan, null, null, indexName);
                    logger.info("Operation "+operation +" completed with result: " + result);
                } else if (operation.equals(operationDelete)) {
                    String result = deleteDocument(message);
                    logger.info("Operation "+operation+" completed with result: " + result);
                } else {
                    throw new IOException(operation);
                }
            } catch (JsonProcessingException ex){
                logger.error("Unable to process message as JsonNode:"+ ex.getMessage());
                break;
            } catch (IOException ex) {
                logger.error("Unidentified Operation Type detected: "+ ex.getMessage());
                break;
            } catch (NullPointerException ex){
                //logger.error("NullPointerException detected: "+ ex.getMessage());
            }
        }
    }

    private static boolean indexExists() throws IOException {
        GetIndexRequest request = new GetIndexRequest(indexName);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

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

    private static String postDocument(JsonNode jsonNode, String parentId, String ancestorId, String name) {
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
            request.source(generateBuilder(jsonNode, parentId, name));

            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

            checkForNestedObjectsInJsonNode(jsonNode, documentId, ancestorId);
            logger.info("Document with id: "+indexResponse.getId()+" posted.");
            return indexResponse.getResult().name();
        }catch (IOException ex){
            logger.error("Error occurred in creating document"+jsonNode.get(plan_objid).asText()+": "+ex.getMessage());
            ex.printStackTrace();
            return null;
        }
    }

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
                    postArrDocument(jsonNode.get(plan_lps), documentId, ancestorId);
                    break;
                default:
                    logger.info("Plain-text key "+key+"-"+e.getValue()+"skipped in checking nested objects.");
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
            if(parentId==null){
                builder.startObject("plan_join").field("name", "plan").endObject();
            }else{
                builder.startObject("plan_join");
                builder.field("parent", parentId);
                builder.field("name", name);
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }catch (IOException ex){
            logger.error("Error occurs when attempting to building XContentBuilder: "+ex.getMessage());
            return null;
        }
    }

    private static void postArrDocument(JsonNode jsonNode, String documentId, String ancestorId) {
        ArrayNode jsonArray = (ArrayNode) jsonNode;
        jsonArray.forEach(jn -> postDocument(jn, documentId, ancestorId, plan_lps));
    }

    private static String deleteDocument(String documentId) throws IOException {
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
