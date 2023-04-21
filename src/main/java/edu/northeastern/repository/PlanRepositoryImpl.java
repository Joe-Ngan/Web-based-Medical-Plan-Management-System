package edu.northeastern.repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import edu.northeastern.utils.JsonUtils;
import org.springframework.stereotype.Repository;
import redis.clients.jedis.JedisPooled;

import java.util.*;


@Repository
public class PlanRepositoryImpl<T> implements PlanRepository<T> {

    private static final String hostname = "localhost";
    private static final Integer redis_port = 6379;
    private final JedisPooled jedis = new JedisPooled(hostname, redis_port);

    @Override
    public void putValue(String key, String value) {
        jedis.set(key, value);
    }

    @Override
    public String getValue(String key) {
        return jedis.get(key);
    }


    @Override
    public Long deleteValue(String key) {
        return jedis.del(key);
    }

    @Override
    public void traverseInput(JsonNode jsonNode) {
        jsonNode.fields().forEachRemaining(e -> {
            JsonNode value = e.getValue();
            if(value.isObject()){
                traverseInput(value);
                flattenEntry(e);
            }else if(value.isArray()){
                List<JsonNode> tmp = new ArrayList<>();
                Iterator<JsonNode> iterator = value.iterator();
                while(iterator.hasNext()){
                    JsonNode cur = iterator.next();
                    if(cur.isContainerNode())traverseInput(cur);
                    tmp.add(flatten(cur));
                    traverseInput(cur);
                }
                if(!tmp.isEmpty()){
                    ((ArrayNode)e.getValue()).removeAll();
                    tmp.forEach(s -> {
                        if(s!=null)((ArrayNode)e.getValue()).add(s);
                    });
                }
            }
        });
    }

    private JsonNode flatten(JsonNode node) {
        String objectType = node.get("objectType").asText();
        String objectId = node.get("objectId").asText();
        String id = "id_" + objectType + "_" + objectId;

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        JsonNode flattenNode = mapper.valueToTree(id);
        //store every object in redis: key(id_objectType_objectId)-value(JsonNode)
        putValue(id, node.toString());
        return flattenNode;
    }

    private void flattenEntry(Map.Entry<String, JsonNode> entry) {
        entry.setValue(flatten(entry.getValue()));
    }

    @Override
    public void populateNestedData(JsonNode parent, Set<String> childIdSet) {
        if (parent == null) return;
        while (parent.toString().contains("id_")) {
            parent.fields().forEachRemaining(e -> {
                JsonNode value = e.getValue();
                if (value.isArray()) {
                    List<JsonNode> tmp = new ArrayList<>();
                    Iterator<JsonNode> iterator = value.iterator();
                    while(iterator.hasNext()){
                        JsonNode cur = iterator.next();
                        if (cur.asText().startsWith("id_"))tmp.add(cur);
                        if (cur.isContainerNode()) populateNestedData(cur, childIdSet);
                        //iterate in Iterator
                        cur.iterator().forEachRemaining(innerNode -> {
                            if (innerNode.isContainerNode())
                                populateNestedData(cur, childIdSet);
                        });
                    }
                    if (!tmp.isEmpty()) {
                        ((ArrayNode) e.getValue()).removeAll();
                        tmp.forEach(s -> {
                            if (childIdSet != null)childIdSet.add(s.asText());
                            String originVal = getValue(s.asText());
                            if (originVal != null){
                                ((ArrayNode) e.getValue()).add(JsonUtils.stringToNode(originVal));
                            }
                        });
                    }
                }else if (value.asText().startsWith("id_")) {
                    if (childIdSet != null) childIdSet.add(value.asText());
                    String originVal = getValue(value.asText());
                    if(originVal == null) originVal="";
                    e.setValue(JsonUtils.stringToNode(originVal));
                }
            });
        }
    }

    @Override
    public List<String> deleteValueTraverse(String id) {
        Set<String> childIdSet = new HashSet<>();
        childIdSet.add(id);

        populateNestedData(JsonUtils.stringToNode(getValue(id)), childIdSet);

        List<String> undeleted = new ArrayList<>();
        for(String childId: childIdSet){
            if(deleteValue(childId)==0)undeleted.add(id);
        }
        return undeleted;

    }
}
