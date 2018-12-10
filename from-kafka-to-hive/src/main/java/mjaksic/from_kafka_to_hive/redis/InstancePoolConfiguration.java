package mjaksic.from_kafka_to_hive.redis;

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
