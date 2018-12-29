package mjaksic.Hive_Redis_To_Spark_To_Hive.redis;

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
