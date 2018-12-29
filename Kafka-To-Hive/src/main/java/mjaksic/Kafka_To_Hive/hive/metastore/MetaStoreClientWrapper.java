package mjaksic.Kafka_To_Hive.hive.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

import mjaksic.Kafka_To_Hive.hive.config.MetaStoreClientWrapperConfiguration;
import mjaksic.Kafka_To_Hive.hive.config.TableInDatabase;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

/**
 * Talks to the Apache Hive metastore.
 */
public class MetaStoreClientWrapper implements Closeable {
    private static final int exception_exit_code = -4;

    private MetaStoreClientWrapperConfiguration config;

    private HiveMetaStoreClient metastore;

    /**
     *
     * @param config See the link below.
     * @see MetaStoreClientWrapperConfiguration
     */
    public MetaStoreClientWrapper(MetaStoreClientWrapperConfiguration config) {
        SetConfig(config);
        SetMetaStoreClientWrapper();
    }

    private void SetConfig(MetaStoreClientWrapperConfiguration config) {
        this.config = config;
    }

    /**
     * Attempts to create a metastore client.
     */
    private void SetMetaStoreClientWrapper() {
        try {
            HiveConf config = CreateHiveConfiguration();
            HiveMetaStoreClient metastore = new HiveMetaStoreClient(config);
            this.metastore = metastore;
        } catch (MetaException e) {
            e.printStackTrace();
            ShutDownAfterException();
        }
    }

    /**
     * Set HiveConf values after instantiation.
     * @return Config.
     * @see <a href="https://hive.apache.org/javadocs/r2.1.1/api/org/apache/hadoop/hive/conf/HiveConf.html">Hive Conf</a>
     */
    private HiveConf CreateHiveConfiguration() {
        HiveConf hive_configuration = new HiveConf(MetaStoreClientWrapper.class);
        hive_configuration.setVar(HiveConf.ConfVars.METASTOREURIS, this.config.hive_metastore_uris);

        return hive_configuration;
    }



    /**
     *
     * @param table_in_database A Hive table in a database. See the link below.
     * @return Names of table columns.
     * @see bigdata.hive.config.TableInDatabase
     */
    public List<String> GetNamesOfColumns(TableInDatabase table_in_database) {
        List<FieldSchema> fields = GetColumnFields(table_in_database);

        ArrayList<String> columns = new ArrayList<>();
        for (FieldSchema field:fields) {
            columns.add(field.getName());
        }
        return columns;
    }

    /**
     *
     * @param table_in_database A Hive table in a database. See the link below.
     * @return Fields which describe the table columns.
     * @see bigdata.hive.config.TableInDatabase
     */
    public List<FieldSchema> GetColumnFields(TableInDatabase table_in_database) {
        List<FieldSchema> fields = null;
        try {
            fields = this.metastore.getFields(table_in_database.getDatabase_name(), table_in_database.getTable_name());
        } catch (TException e) {
            e.printStackTrace();
        }
        return fields;
    }



    private void ShutDownAfterException() {
        System.out.println("Shutting down after an exception.");
        System.exit(MetaStoreClientWrapper.exception_exit_code);
    }

    /**
     * You may want to use try-with-resources.
     */
    @Override
    public void close() {
        this.metastore.close();
    }
}