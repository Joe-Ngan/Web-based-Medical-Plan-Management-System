package edu.northeastern.repository;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Set;

public interface PlanRepository<T> {

    void putValue(String key, String value);

    String getValue(String key);

    Long deleteValue(String key);

    void traverseInput(JsonNode jsonNode);

    void populateNestedData(JsonNode parent, Set<String> childIdSet);

    List<String> deleteValueTraverse(String id);

}
