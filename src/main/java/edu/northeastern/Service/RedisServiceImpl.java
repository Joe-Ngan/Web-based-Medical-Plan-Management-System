package edu.northeastern.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import edu.northeastern.excpetions.ResourceNotFoundException;
import edu.northeastern.repository.PlanRepository;
import edu.northeastern.utils.JsonUtils;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class RedisServiceImpl implements RedisService{

    @Autowired
    PlanRepository<String> planRepository;

    @Override
    public String getValue(String key) {
        return planRepository.getValue(key);
    }

    @Override
    public void postValue(String key, String value) {
        planRepository.putValue(key, value);
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
        planRepository.putValue(id, node.toString());
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
                            String originVal = planRepository.getValue(s.asText());
                            if (originVal != null){
                                ((ArrayNode) e.getValue()).add(JsonUtils.stringToNode(originVal));
                            }
                        });
                    }
                }else if (value.asText().startsWith("id_")) {
                    if (childIdSet != null) childIdSet.add(value.asText());
                    String originVal = planRepository.getValue(value.asText());
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
            if(planRepository.deleteValue(childId)==0)undeleted.add(id);
        }
        return undeleted;

    }
}
