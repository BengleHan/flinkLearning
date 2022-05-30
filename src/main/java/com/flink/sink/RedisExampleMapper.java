package com.flink.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import redis.clients.jedis.JedisPoolConfig;

public class RedisExampleMapper implements RedisMapper<Tuple2<String, String>> {

    private static String REDIS_ADDITIONAL_KEY="redis_addtional_key";
    private static String REDIS_HOST="192.168.1.18";
    private static String REDIS_PORT="6379";

    private RedisCommand redisCommand;

    public RedisExampleMapper(RedisCommand redisCommand){
         this.redisCommand = redisCommand;
     }
     public RedisCommandDescription getCommandDescription() {
         return new RedisCommandDescription(redisCommand, REDIS_ADDITIONAL_KEY);
     }
     public String getKeyFromData(Tuple2<String, String> data) {
         return data.f0;
     }

     public String getValueFromData(Tuple2<String, String> data) {
        return data.f1;
     }

// JedisPoolConfig jedisPoolConfig = new JedisPoolConfig.Builder().setHost(REDIS_HOST).setPort(REDIS_PORT).build();
// new RedisSink<String>(jedisPoolConfig, new RedisExampleMapper(RedisCommand.LPUSH));

    }
