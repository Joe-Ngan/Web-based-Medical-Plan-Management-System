package edu.northeastern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.data.redis.core.RedisTemplate;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPooled;

import java.io.IOException;

@SpringBootApplication
@EnableCaching
public class Demo1Application {

    public static void main(String[] args) {
        SpringApplication.run(Demo1Application.class, args);
        System.out.println("Hello World!");
        addMQListener();
    }

    private static final String hostname = "localhost";
    private static final Integer redis_port = 6379;
    private static final Integer elastic_port = 9200;
    private static final String scheme = "http";
    private static final String indexName="indexplan";

    private static final Logger logger = LoggerFactory.getLogger(Demo1Application.class);
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

    private static final String mapping= "{\r\n" +
            "	\"properties\": {\r\n" +
            "		\"_org\": {\r\n" +
            "			\"type\": \"text\"\r\n" +
            "		},\r\n" +
            "		\"objectId\": {\r\n" +
            "			\"type\": \"keyword\"\r\n" +
            "		},\r\n" +
            "		\"objectType\": {\r\n" +
            "			\"type\": \"text\"\r\n" +
            "		},\r\n" +
            "		\"planType\": {\r\n" +
            "			\"type\": \"text\"\r\n" +
            "		},\r\n" +
            "		\"creationDate\": {\r\n" +
            "			\"type\": \"date\",\r\n" +
            "			\"format\" : \"MM-dd-yyyy\"\r\n" +
            "		},\r\n" +
            "		\"planCostShares\": {\r\n" +
            "			\"type\": \"nested\",\r\n" +
            "			\"properties\": {\r\n" +
            "				\"copay\": {\r\n" +
            "					\"type\": \"long\"\r\n" +
            "				},\r\n" +
            "				\"deductible\": {\r\n" +
            "					\"type\": \"long\"\r\n" +
            "				},\r\n" +
            "				\"_org\": {\r\n" +
            "					\"type\": \"text\"\r\n" +
            "				},\r\n" +
            "				\"objectId\": {\r\n" +
            "					\"type\": \"keyword\"\r\n" +
            "				},\r\n" +
            "				\"objectType\": {\r\n" +
            "					\"type\": \"text\"\r\n" +
            "				}\r\n" +
            "			}\r\n" +
            "		},\r\n" +
            "		\"linkedPlanServices\": {\r\n" +
            "			\"type\": \"nested\",\r\n" +
            "			\"properties\": {\r\n" +
            "				\"_org\": {\r\n" +
            "					\"type\": \"text\"\r\n" +
            "				},\r\n" +
            "				\"objectId\": {\r\n" +
            "					\"type\": \"keyword\"\r\n" +
            "				},\r\n" +
            "				\"objectType\": {\r\n" +
            "					\"type\": \"text\"\r\n" +
            "				},\r\n" +
            "				\"linkedService\": {\r\n" +
            "                   \"type\": \"nested\",\r\n" +
            "					\"properties\": {\r\n" +
            "						\"name\": {\r\n" +
            "							\"type\": \"text\"\r\n" +
            "						},\r\n" +
            "						\"_org\": {\r\n" +
            "							\"type\": \"text\"\r\n" +
            "						},\r\n" +
            "						\"objectId\": {\r\n" +
            "							\"type\": \"keyword\"\r\n" +
            "						},\r\n" +
            "						\"objectType\": {\r\n" +
            "							\"type\": \"text\"\r\n" +
            "						}\r\n" +
            "					}\r\n" +
            "				},\r\n" +
            "				\"planserviceCostShares\": {\r\n" +
            "                  \"type\": \"nested\",\r\n" +
            "					\"properties\": {\r\n" +
            "						\"copay\": {\r\n" +
            "							\"type\": \"long\"\r\n" +
            "						},\r\n" +
            "						\"deductible\": {\r\n" +
            "							\"type\": \"long\"\r\n" +
            "						},\r\n" +
            "						\"_org\": {\r\n" +
            "							\"type\": \"text\"\r\n" +
            "						},\r\n" +
            "						\"objectId\": {\r\n" +
            "							\"type\": \"keyword\"\r\n" +
            "						},\r\n" +
            "						\"objectType\": {\r\n" +
            "							\"type\": \"text\"\r\n" +
            "						}\r\n" +
            "					}\r\n" +
            "				}\r\n" +
            "			}\r\n" +
            "		}\r\n" +
            "	}\r\n" +
            "}";

    private static void addMQListener() {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, elastic_port, scheme)));
        logger.info("Message Queue started at: "+hostname+":"+elastic_port);
        try {
            if (!indexExists()) {
                String index = createElasticIndex();
                logger.info("In dex "+index+ "created.");
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
                String message = job.get(messageField).toString();
                String operation = job.get(operationField).toString();
                logger.info("New Job Message Queue received: Operation: " + operation + ". Message:" + message);

                if (operation.equals(operationPost)) {
                    JsonNode plan = new ObjectMapper().readTree(message);
                    String result = postDocument(plan, null, null, indexName);
                    logger.info("Operation "+operation +"completed with result: " + result);
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
        request.mapping(mapping, XContentType.JSON);
        return client.indices().create(request, RequestOptions.DEFAULT).index();
    }

    private static String postDocument(JsonNode jsonNode, String parentId, String ancestorId, String name) throws IOException {
        if(jsonNode==null) return null;

        String documentId = jsonNode.get(plan_objid).toString();
        JsonNode document = generateDoucment(jsonNode, parentId, name);

        IndexRequest request = new IndexRequest(indexName);
        request.id(documentId);
        request.source(document.toString(), XContentType.JSON);
        if(ancestorId == null) ancestorId = parentId;
        request.routing(ancestorId);

        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        final String ancId = ancestorId;

        jsonNode.fields().forEachRemaining(e -> {
            try{
                switch (e.getKey()){
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
                        logger.info("Plain text key "+e.getKey()+"-"+e.getValue()+"skipped in message body.");
                        break;
                }
            }catch (IOException ex){
                logger.error("Error occurs when attempting to post document with key "+e.getKey());
            }
        });
        logger.info("Document with id: "+indexResponse.getId()+" posted.");
        return indexResponse.getResult().name();
    }

    private static JsonNode generateDoucment(JsonNode jsonNode, String parentId, String name) {

        String objectType = jsonNode.get(plan_objt).asText();
        String objectId = jsonNode.get(plan_objid).asText();
        String org = jsonNode.get(plan_org).asText();

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode planJoin = mapper.createObjectNode();
        ObjectNode document = mapper.createObjectNode();


        if (parentId != null) planJoin.put("parent", parentId);
        planJoin.put("name", name);
        document.set("plan_join", planJoin);

        document.put(plan_objid, objectId);
        document.put(plan_objt, objectType);
        document.put(plan_org, org);

        jsonNode.fields().forEachRemaining(e -> {
            switch (e.getKey()){
                case plan_pt:
                    document.put(plan_pt, e.getValue().toString());
                    break;
                case plan_cd:
                    document.put(plan_cd, e.getValue().toString());
                    break;
                case plan_dd:
                    document.put(plan_dd, e.getValue().toString());
                    break;
                case plan_cp:
                    document.put(plan_cp, e.getValue().toString());
                    break;
                case plan_name:
                    document.put(plan_name, e.getValue().toString());
                    break;
                default:
                    logger.warn("Unexpected key-value "+e.getKey()+"-"+e.getValue()+"skipped in generateDoucment.");
                    break;
            }
        });
        return document;
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
