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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    private static final Integer numOfShards = 1;
    private static final String indexReplicas = "index.number_of_replicas";
    private static final Integer numOfReplicas = 1;

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
                logger.info("Index "+index+ " created.");
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
        boolean exist = client.indices().exists(request, RequestOptions.DEFAULT);
        return exist;
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

    private static String postDocument(JsonNode jsonNode, String parentId, String ancestorId, String name) throws IOException {
        if(jsonNode==null) return null;
        IndexRequest request = new IndexRequest(indexName);
        String documentId = jsonNode.get(plan_objid).asText();
        String documentType = jsonNode.get(plan_objt).asText();

        request.id(documentId);
        if(ancestorId == null) ancestorId = parentId;
        request.routing(ancestorId);

        /**------------new builder-------------**/
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field(plan_org, jsonNode.get(plan_org).asText());
        builder.field(plan_objt, jsonNode.get(plan_objt).asText());
        builder.field(plan_objid, jsonNode.get(plan_objid).asText());
        if(jsonNode.get(plan_pt)!=null) builder.field(plan_pt, jsonNode.get(plan_pt).asText());
        if(jsonNode.get(plan_cd)!=null) builder.field(plan_cd, jsonNode.get(plan_cd).asText());
        if(jsonNode.get(plan_cp)!=null) builder.field(plan_cp, jsonNode.get(plan_cp).asText());
        if(jsonNode.get(plan_dd)!=null) builder.field(plan_dd, jsonNode.get(plan_dd).asText());
        if(jsonNode.get(plan_name)!=null) builder.field(plan_name, jsonNode.get(plan_name).asText());
        if(parentId==null){
            builder.startObject("plan_join").field("name", "plan").endObject();
        }else{
            builder.startObject("plan_join");
            builder.field("parent", parentId);
            builder.field("name", name);
            builder.endObject();
        }
        builder.endObject();
        request.source(builder);
        /**------------to solve the problem-------------**/
        //JsonNode document = generateDocument(jsonNode, parentId, ancestorId, name);
        //request.source(document.toString(), XContentType.JSON);
        /**------------to replace the old version-------------**/

        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        final String ancId = ancestorId;

        jsonNode.fields().forEachRemaining(e -> {
            String key = e.getKey();
            try{
                switch (key){
                    case plan_pcs:
                        postDocument(jsonNode.get(plan_pcs), documentId, ancId, plan_pcs);
                        break;
                    case plan_ls:
                        postDocument(jsonNode.get(plan_ls), documentId, ancId, plan_ls);
                        break;
                    case plan_pscs:
                        postDocument(jsonNode.get(plan_pscs), documentId, ancId, plan_pscs);
                        break;
                    case plan_lps:
                        postArrDocument(jsonNode.get(plan_lps), documentId, ancId);
                        break;
                    default:
                        logger.info("Plain text key "+key+"-"+e.getValue()+"skipped in processing the objects and arrays in postDocument");
                        break;
                }
            }catch (IOException ex){
                logger.error("Error occurs when attempting to post document with key: "+key);
            }
        });
        logger.info("Document with id: "+indexResponse.getId()+" posted.");
        return indexResponse.getResult().name();
    }

    private static JsonNode generateDocument(JsonNode jsonNode, String parentId,String ancestorId, String name) {
        String documentId = jsonNode.get(plan_objid).asText();
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode planJoin = mapper.createObjectNode();
        ObjectNode document = mapper.createObjectNode();
        //parent-child relationship set up
        if (parentId != null) planJoin.put("parent", parentId);
        planJoin.put("name", name);
        document.set("plan_join", planJoin);

//        {
//            "title":"Parent Document",
//            "join_field":{
//                "name":"parent",
//                "parent":""
//            }
//        }

        //for the plain key value pairs
        jsonNode.fields().forEachRemaining(e -> {
            switch (e.getKey()){
                case plan_pt:
                    document.put(plan_pt, e.getValue().asText());
                    break;
                case plan_cd:
                    document.put(plan_cd, e.getValue().asText());
                    break;
                case plan_dd:
                    document.put(plan_dd, e.getValue().longValue());
                    break;
                case plan_cp:
                    document.put(plan_cp, e.getValue().longValue());
                    break;
                case plan_name:
                    document.put(plan_name, e.getValue().asText());
                    break;
                case plan_org:
                    document.put(plan_org, e.getValue().asText());
                    break;
                case plan_objid:
                    document.put(plan_objid, e.getValue().asText());
                    break;
                case plan_objt:
                    document.put(plan_objt, e.getValue().asText());
                    break;
//                case plan_pcs:
//                    try {
//                        postDocument(jsonNode.get(plan_pcs), documentId, ancestorId, plan_pcs);
//                    } catch (IOException ex) {
//                        ex.printStackTrace();
//                    }
//                    break;
//                case plan_ls:
//                    try {
//                        postDocument(jsonNode.get(plan_ls), documentId, ancestorId, plan_ls);
//                    } catch (IOException ex) {
//                        ex.printStackTrace();
//                    }
//                    break;
//                case plan_pscs:
//                    try {
//                        postDocument(jsonNode.get(plan_pscs), documentId, ancestorId, plan_pscs);
//                    } catch (IOException ex) {
//                        ex.printStackTrace();
//                    }
//                    break;
//                case plan_lps:
//                    postArrDocument(jsonNode.get(plan_lps), documentId, ancestorId);
//                    break;
                default:
                    logger.warn("Unexpected key-value "+e.getKey()+"-"+e.getValue()+"skipped in processing the plain key value pairs in generateDocument.");
                    break;
            }
        });
        return document;
    }

    private static List<String> getObjectListId(JsonNode value) {
        List<String> idSet = new ArrayList<>();
        ArrayNode arr = (ArrayNode) value;
        arr.forEach(obj -> {
            try {
                JsonNode id = obj.get(plan_objid);
                if(id.isEmpty()) throw new IOException("Fail to retrieve ids of "+value.asText());
                idSet.add(id.asText());
            } catch (IOException ex) {
                logger.error("[generateDocument]"+ex.getMessage());
                ex.printStackTrace();
            }
        });
        return idSet;
    }

    private static String getObjectId(JsonNode value) {
        try{
            JsonNode id = value.get(plan_objid);
            if(id.isEmpty()) throw new IOException("Fail to retrieve id of "+value.asText());
            return id.asText();
        }catch (IOException ex) {
            logger.error("[generateDocument]"+ex.getMessage());
            ex.printStackTrace();
            return null;
        }
    }

    private static void postArrDocument(JsonNode jsonNode, String documentId, String ancestorId) {
        ArrayNode jsonArray = (ArrayNode) jsonNode;
        jsonArray.forEach(jn -> {
            try {
                postDocument(jn, documentId, ancestorId, plan_lps);
            } catch (IOException e) {
                logger.error("Error occurs when attempting to post linkedPlanServices with JsonNode "+ jn.asText());
            }
        });
    }

    private static String deleteDocument(String documentId) throws IOException {
        DeleteRequest request = new DeleteRequest(indexName, documentId);
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        logger.info("Document with id: "+response.getId()+" deleted.");
        return response.toString();
    }
}
