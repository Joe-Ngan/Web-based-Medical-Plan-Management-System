package edu.northeastern.repository;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface PlanRepository<T> {
    public void putValue(String key, T value);

    public void putValueWithExpireTime(String key, T value, long timeout, TimeUnit unit);

    public T getValue(String key);

    public T getHash(String key);

    public boolean deleteValue(String key);

    public void putMapEntry(String redisKey, Object key, T data);

    public T getMapValue(String redisKey, Object key);

    public Map<Object, T> getMapEntries(String redisKey);

    public void setExpire(String key, long timeout, TimeUnit unit);

    public void publish(String key, T value);
}
