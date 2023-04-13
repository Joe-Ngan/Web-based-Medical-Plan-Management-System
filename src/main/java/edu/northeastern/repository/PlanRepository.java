package edu.northeastern.repository;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface PlanRepository<T> {

    public void putValue(String key, String value);

    public String getValue(String key);

    public void hSet(String key, String field, String value);

    public String hGet(String key, String field);

    public Map<String, String> hGetAll(String key);

    public void lpush(String key, String value);

    public boolean existsKey(String key);

    public Set<String> getKeysByPattern(String pattern);

    public Long deleteValue(String key);

}
