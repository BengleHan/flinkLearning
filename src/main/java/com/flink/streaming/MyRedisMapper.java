package com.flink.streaming;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {
    //表示从接收的数据中获取需要操作的Redis Key
    @Override
    public String getKeyFromData(Tuple2<String, String> data) {
        return data.f0;
    }
    //表示从接收的数据中获取需要操作的Redis Value
    @Override
    public String getValueFromData(Tuple2<String, String> data) {
        return data.f1;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.LPUSH);
    }
}

