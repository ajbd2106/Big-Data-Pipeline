package mjaksic.from_hive_to_kafka.runners;

import java.util.List;

import mjaksic.from_hive_to_kafka.hive.config.MetaStoreClientWrapperConfiguration;
import mjaksic.from_hive_to_kafka.hive.config.TableInDatabase;
import mjaksic.from_hive_to_kafka.hive.metastore.MetaStoreClientWrapper;

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
