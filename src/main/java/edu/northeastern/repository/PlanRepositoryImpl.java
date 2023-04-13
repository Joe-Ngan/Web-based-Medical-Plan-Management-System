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
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
    public void hSet(String key, String field, String value) {
        jedis.hset(key, field, value);
    }

    @Override
    public String hGet(String key, String field) {
        return jedis.hget(key, field);
    }

    @Override
    public Map<String, String> hGetAll(String key) {
        return jedis.hgetAll(key);
    }

    @Override
    public void lpush(String key, String value) { jedis.lpush(key, value); }

    @Override
    public boolean existsKey(String key) {
        return jedis.exists(key);
    }

    @Override
    public Set<String> getKeysByPattern(String pattern) {
        return jedis.keys(pattern);
    }

    @Override
    public Long deleteValue(String key) {
        return jedis.del(key);
    }

}
