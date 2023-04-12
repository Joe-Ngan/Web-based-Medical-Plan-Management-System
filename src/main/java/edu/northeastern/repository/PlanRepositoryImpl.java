package edu.northeastern.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Repository;
import redis.clients.jedis.JedisPooled;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Repository
public class PlanRepositoryImpl<T> implements PlanRepository<T> {

    private RedisTemplate<String, T> redisTemplate;
    private HashOperations<String, Object, T> hashOperation;
    private ValueOperations<String, T> valueOperations;

    @Autowired
    PlanRepositoryImpl(RedisTemplate<String, T> redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.hashOperation = redisTemplate.opsForHash();
        this.valueOperations = redisTemplate.opsForValue();
        this.listOperations = redisTemplate.opsForList();
    }

    @Override
    public void putValue(String key, T value) {
        valueOperations.set(key, value);
        valueOperations.set(key+"_hash", (T)String.valueOf(System.currentTimeMillis()));
    }

    @Override
    public void putValueWithExpireTime(String key, T value, long timeout, TimeUnit unit) {
        valueOperations.set(key, (T) value, timeout, unit);
    }

    @Override
    public T getValue(String key) {
        return valueOperations.get(key);
    }

    @Override
    public T getHash(String key) {
        return valueOperations.get(key+"_hash");
    }

    @Override
    public boolean deleteValue(String key) {
        return redisTemplate.delete(key);
    }

    @Override
    public void putMapEntry(String redisKey, Object key, T data) {
        hashOperation.put(redisKey, key, data);
    }

    @Override
    public T getMapValue(String redisKey, Object key) {
        return hashOperation.get(redisKey, key);
    }

    @Override
    public Map<Object, T> getMapEntries(String redisKey) {
        return hashOperation.entries(redisKey);
    }

    @Override
    public void setExpire(String key, long timeout, TimeUnit unit) {
        redisTemplate.expire(key, timeout, unit);
    }

    @Override
    public void publish(String key, T value){
        redisTemplate.convertAndSend(key, value);
    }
}
