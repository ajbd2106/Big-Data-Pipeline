package mjaksic.Kafka_To_Hive.hive.config;

/**
 * Configures the Hive metastore client.
 */
public class MetaStoreClientWrapperConfiguration {
    public String hive_metastore_uris;

    /**
     *
     * @param hive_metastore_uris A Hive metastore URI. See the link below.
     * @see "Look up hive.metastore.uris in /path/to/hive/.../hive-site.xml"
     */
    public MetaStoreClientWrapperConfiguration(String hive_metastore_uris) {
        this.hive_metastore_uris = hive_metastore_uris;
    }
}