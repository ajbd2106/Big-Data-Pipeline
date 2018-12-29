package mjaksic.Hive_Redis_To_Spark_To_Hive.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Closeable;

/**
 * Handles the Redis instance pool.
 * The pool is implemented as a singleton.
 */
public class InstancePool implements Closeable {
    public final static InstancePool POOL = new InstancePool();

    private InstancePoolConfiguration config;

    private JedisPool pool;

    /**
     * Defeats instantiation. Part of a thread-safe singleton construction.
     */
    public InstancePool() {

    }

    /**
     *
     * @param config Set the resource pool configuration, once. Can be null. See the link below.
     * @return Redis instance from a pool of instances.
     * @see bigdata.redis.InstancePoolConfiguration
     */
    public Jedis GetRedisInstance(InstancePoolConfiguration config) { //TODO, decouple configuration from asking for an instance while also preserving the singleton construction
        if (this.config == null) {
            SetConfig(config);
            SetRedisPool();
        }
        return this.pool.getResource();
    }

    private void SetConfig(InstancePoolConfiguration config) {
        this.config = config;
    }

    private void SetRedisPool() {
        JedisPool pool = new JedisPool(new JedisPoolConfig(), this.config.host);

        this.pool = pool;
    }

    /**
     * You may want to use try-with-resources.
     */
    @Override
    public void close() {
        this.pool.close();
    }
}
