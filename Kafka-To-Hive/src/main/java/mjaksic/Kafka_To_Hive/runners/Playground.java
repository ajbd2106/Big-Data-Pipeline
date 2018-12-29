package mjaksic.Kafka_To_Hive.runners;

import java.util.List;

import mjaksic.Kafka_To_Hive.hive.config.MetaStoreClientWrapperConfiguration;
import mjaksic.Kafka_To_Hive.hive.config.TableInDatabase;
import mjaksic.Kafka_To_Hive.hive.metastore.MetaStoreClientWrapper;

/**
 * For playing around with different classes, to see if they work, for manually checking their output.
 */
public class Playground {

    public static void main(String[] args) {
        MetaStoreClientWrapperConfiguration config = new MetaStoreClientWrapperConfiguration("thrift://sandbox-hdp.hortonworks.com:9083");
        MetaStoreClientWrapper metastore = new MetaStoreClientWrapper(config);
        TableInDatabase table_in_database = new TableInDatabase("default", "unpartitioned_alarms_raw");
        List<String> columns = metastore.GetNamesOfColumns(table_in_database);
        metastore.close();
        System.out.println(columns.toString());
    }
}
