package mjaksic.from_hive_redis_to_spark_to_hive.redis;

/**
 * Configures Redis.
 * @see <a href="https://github.com/xetorthio/jedis/wiki">RedisClient; Jedis GitHub Wiki</a>
 */
public class InstancePoolConfiguration {
    public String host;

    public InstancePoolConfiguration(String host) {
        this.host = host;
    }
}
