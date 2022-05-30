package com.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class RedisSink<IN> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);
/**
 * This additional key needed for {@link RedisDataType#HASH} and {@linkRedisDataType#SORTED_SET}.
* Other {@link RedisDataType} works only with two variable i.e. name of the
 * list and value to be added.
 *      * But for {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET} we need three variables.
 *      * <p>For {@link RedisDataType#HASH} we need hash name, hash key and element.
 *      * {@code additionalKey} used as hash name for {@link RedisDataType#HASH}
 *      * <p>For {@link RedisDataType#SORTED_SET} we need set name, the element and it's score.
 *      * {@code additionalKey} used as set name for {@link RedisDataType#SORTED_SET}
 *      */

    private String additionalKey;

    private RedisMapper<IN> redisSinkMapper;

    private RedisCommand redisCommand;


    private org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase FlinkJedisConfigBase;

    private RedisCommandsContainer redisCommandsContainer;

 /**
 *      * Creates a new {@link RedisSink} that connects to the Redis server.
 *      *
 *      * @param FlinkJedisConfigBase The configuration of {@link FlinkJedisConfigBase}
 *      * @param redisSinkMapper This is used to generate Redis command and key value
 *      from incoming elements.
 **/


    public RedisSink(FlinkJedisConfigBase FlinkJedisConfigBase, RedisMapper<IN>
              redisSinkMapper) {
              Objects.requireNonNull(FlinkJedisConfigBase, "Redis connection pool config should not be null ");
                 Objects.requireNonNull(redisSinkMapper, "Redis Mapper can not be null");
              Objects.requireNonNull(redisSinkMapper.getCommandDescription(), "Redis Mapper data type description can not be null ");

              this.FlinkJedisConfigBase = FlinkJedisConfigBase;

              this.redisSinkMapper = redisSinkMapper;
              RedisCommandDescription redisCommandDescription = redisSinkMapper.getCommandDescription();
              this.redisCommand = redisCommandDescription.getCommand();
              this.additionalKey = redisCommandDescription.getAdditionalKey();
    }
  /**
   * Called when new data arrives to the sink, and forwards it to Redis channel.
   * Depending on the specified Redis data type (see {@link RedisDataType}),
   *  a different Redis command will be applied.
   *      * Available commands are RPUSH, LPUSH, SADD, PUBLISH, SET, PFADD, HSET, ZADD.
   *      * @param input The incoming data
   *      */

    @Override


    public void invoke(IN input) throws Exception {
        String key = redisSinkMapper.getKeyFromData(input);
        String value = redisSinkMapper.getValueFromData(input);

        switch (redisCommand) {
            case RPUSH:
                this.redisCommandsContainer.rpush(key, value);
                break;
            case LPUSH:
                this.redisCommandsContainer.lpush(key, value);
                break;
            case SADD:
                this.redisCommandsContainer.sadd(key, value);
                break;
            case SET:
                this.redisCommandsContainer.set(key, value);
                break;
            case PFADD:
                this.redisCommandsContainer.pfadd(key, value);
                break;
            case PUBLISH:
                this.redisCommandsContainer.publish(key, value);
                break;
            case ZADD:
                this.redisCommandsContainer.zadd(this.additionalKey, value, key);
                break;
            case ZREM:
                this.redisCommandsContainer.zrem(this.additionalKey, key);
                break;
            case HSET:
                this.redisCommandsContainer.hset(this.additionalKey, key, value);
                break;
            default:
                throw new IllegalArgumentException("Cannot process such data type: " + redisCommand);
        }
    }
    /**
     * Initializes the connection to Redis by either cluster or sentinels or single server.
     * @throws IllegalArgumentException if jedisPoolConfig, jedisClusterConfig and
    jedisSentinelConfig are all null
    */
    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.FlinkJedisConfigBase);
            this.redisCommandsContainer.open();
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    /**
     * Closes commands container.* @throws IOException if command container is unable to close.
     *      */

    @Override
    public void close() throws IOException {
         if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
         }
    }

}

