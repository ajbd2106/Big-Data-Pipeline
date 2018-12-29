package mjaksic.Hive_Redis_To_Spark_To_Hive.spark;

import org.apache.spark.sql.*;

import mjaksic.Hive_Redis_To_Spark_To_Hive.json.Parser;
import mjaksic.Hive_Redis_To_Spark_To_Hive.redis.InstancePool;
import mjaksic.Hive_Redis_To_Spark_To_Hive.redis.InstancePoolConfiguration;
import mjaksic.Hive_Redis_To_Spark_To_Hive.sql.SelectConfiguration;
import mjaksic.Hive_Redis_To_Spark_To_Hive.sql.SelectConstructor;
import redis.clients.jedis.Jedis;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

/**
 * Interacts with Apache Spark and Hive.
 */
public class SparkHiveAmalgamation implements Closeable {
    private static final String key_columns = "ip,alarm_type";
    private static final String key_table = "unpartitioned_alarms_raw";

    private static final String data_columns = "stamp,state,state_code,info,year,month,ip,alarm_type";
    private static final String data_table = "unpartitioned_alarms_raw";
    private static final String data_order_by = "stamp";

    private static final String string_data_sentinel = "NONE";

    private static final String redis_host = "192.168.50.56";

    private SparkHiveSessionConfiguration config;
    private SparkSession spark_session;

    /**
     *
     * @param config See the link below.
     * @see bigdata.spark.SparkHiveSessionConfiguration
     */
    public SparkHiveAmalgamation(SparkHiveSessionConfiguration config) {
        SetSparkConfig(config);
        SetSparkSession();
    }

    private void SetSparkConfig(SparkHiveSessionConfiguration config) {
        this.config = config;
    }

    private void SetSparkSession() {
        SparkSession spark_session = SparkSession.builder()
                .master(this.config.master)
                .appName(this.config.app_name)
                .config("spark.sql.warehouse.dir", this.config.spark_sql_warehouse_dir)
                .config("hive.metastore.uris", this.config.hive_metastore_uris)
                .config("hive.exec.dynamic.partition.mode", this.config.hive_exec_dynamic_partition_mode)
                .enableHiveSupport()
                .getOrCreate();

        this.spark_session = spark_session;
    }



    /**
     * Executes the time stamp pairing algorithm.
     */
    public void Run() {
        List rows_of_keys = GetAllIPAlarmTypeKeys();

        for (Object key_as_object:rows_of_keys) {
            Row key = (Row) key_as_object;

            Map<String, List> not_up_and_up_alarms = GetNotUpAndUpAlarms(key);
            List<String> enriched_alarms = CreateEnrichedAlarmPairs(not_up_and_up_alarms);

            WriteIntoHive(enriched_alarms);
        }
    }

    private List GetAllIPAlarmTypeKeys() {
        SelectConfiguration ip_type_config = new SelectConfiguration(SparkHiveAmalgamation.key_columns,
                SparkHiveAmalgamation.key_table,
                SelectConstructor.getSql_sentinel(),
                SelectConstructor.getSql_sentinel(),
                SelectConstructor.getSql_sentinel(),
                SelectConstructor.getSql_sentinel());
        String sql_string = SelectConstructor.ConstructSQLString(ip_type_config);

        Dataset<Row> ip_and_type_rows = ExecuteSparkSQL(sql_string);

        String[] filter_columns = key_columns.split(","); //TODO, clean up the data so that you don't have to filter it
        for (String column:filter_columns) {
            ip_and_type_rows = FilterDatasetByColumnNotEqualTo(ip_and_type_rows, column, SparkHiveAmalgamation.string_data_sentinel);
        }

        return CollectIntoMemory(ip_and_type_rows);
    }

    private Map<String, List> GetNotUpAndUpAlarms (Row key) {
        Dataset<Row> all_alarms = GetAlarms(key);
        Map<String, List> not_up_and_up_alarms = SplitAlarmsIntoNotUpAndUp(all_alarms);

        return not_up_and_up_alarms;
    }

    private Dataset<Row> GetAlarms(Row key) {
        String where = "ip=" + "'" + key.get(0)+ "'" + " and " + "alarm_type=" + "'" + key.get(1) + "'";
        SelectConfiguration key_where_order_config = new SelectConfiguration(SparkHiveAmalgamation.data_columns,
                SparkHiveAmalgamation.data_table,
                where,
                SelectConstructor.getSql_sentinel(),
                SparkHiveAmalgamation.data_order_by,
                SelectConstructor.getSql_sentinel());
        String sql_string = SelectConstructor.ConstructSQLString(key_where_order_config);

        Dataset<Row> alarms = ExecuteSparkSQL(sql_string);

        return FilterDatasetByColumnInNotNull(alarms, SparkHiveAmalgamation.data_order_by);
    }

    private Map<String, List> SplitAlarmsIntoNotUpAndUp(Dataset<Row> all_alarms) {
        Dataset<Row> not_up_alarms = FilterDatasetByColumnNotEqualTo(all_alarms, "state_code", "UP");
        Dataset<Row> up_alarms = FilterDatasetByColumnEqualTo(all_alarms, "state_code", "UP");

        List list_of_not_up_alarms = CollectIntoMemory(not_up_alarms);
        List list_of_up_alarms = CollectIntoMemory(up_alarms);

        HashMap<String, List> not_up_and_up_alarms = new HashMap<>();
        not_up_and_up_alarms.put("not_up_alarms", list_of_not_up_alarms);
        not_up_and_up_alarms.put("up_alarms", list_of_up_alarms);

        return not_up_and_up_alarms;
    }



    private Dataset<Row> FilterDatasetByColumnEqualTo(Dataset<Row> dataset, String column, String value) {
        return dataset.filter(col(column).equalTo(value));
    }

    private Dataset<Row> FilterDatasetByColumnNotEqualTo(Dataset<Row> dataset, String column, String value) {
        return dataset.filter(col(column).notEqual(value));
    }

    /**
     * Filters all non nulls.
     * TIMESTAMP, INT and similar data types that don't have a String "NONE" as a value have null instead.
     * @param dataset Dataset.
     * @param column Column name.
     * @return Return rows without null.
     */
    private Dataset<Row> FilterDatasetByColumnInNotNull(Dataset<Row> dataset, String column) {
        return dataset.filter(column + " is not null");
    }

    private Dataset<Row> ExecuteSparkSQL(String sql_string) {
        return this.spark_session.sql(sql_string);
    }

    /**
     * Warning: moves data into the application's driver process! May cause OutOfMemoryError! See the link below.
     * @param sql_result Spark Dataset.
     * @return A list of rows.
     * @see <a href="https://spark.apache.org/docs/2.1.0/api/java/org/apache/spark/sql/Dataset.html#collectAsList()">CollectAsList</a>
     */
    private List CollectIntoMemory(Dataset<Row> sql_result) {
        List list_of_rows = sql_result.collectAsList();

        return list_of_rows;
    }

    private List<String> CreateEnrichedAlarmPairs(Map<String, List> alarms){
        List not_up_alarms = alarms.get("not_up_alarms");
        List up_alarms = alarms.get("up_alarms");

        Map<String, String> common_data = ExtractCommonData(not_up_alarms);

        int pairs = CalculateNumberOfPairs(not_up_alarms, up_alarms);
        ArrayList<String> enriched_alarms = new ArrayList<>();
        for (int i = 0; i < pairs; i++) {
            Row not_up_alarm = (Row) not_up_alarms.get(i);
            Row up_alarm = (Row) up_alarms.get(i);

            String time_pair = GetAlarmTimePair(not_up_alarm, up_alarm);

            String enriched_csv_row = ConstructEnrichedRow(time_pair, common_data.get("misc_data"), common_data.get("redis_inventory"), common_data.get("partitions"));
            enriched_alarms.add(enriched_csv_row);
        }
        return enriched_alarms;
    }

    private Map<String, String> ExtractCommonData(List alarms){
        Row common_data = (Row) alarms.get(0);

        String misc_data = GetMiscData(common_data);
        String redis_inventory = GetRedisInventory(common_data);
        String partitions = GetPartitionData(common_data);

        HashMap<String, String> data = new HashMap<>();
        data.put("misc_data", misc_data);
        data.put("redis_inventory", redis_inventory);
        data.put("partitions", partitions);

        return data;
    }

    private String GetMiscData(Row alarm) {
        /*
        "stamp,                state, state_code, info,                                      year, month, ip,           alarm_type"
        [2018-06-13 15:19:19.0,SOFT,  CRITICAL,   CRITICAL - Socket timeout after 10 seconds,2018, JUNE,  10.204.64.221,SERVICE]
         0                     1      2           3                                          4     5      6             7
         */
        StringBuilder other_data = new StringBuilder();
        String info = alarm.get(3).toString();
        other_data.append(info);
        other_data.append(",");
        other_data.append("what_is_service");

        return other_data.toString();
    }

    private String GetRedisInventory(Row not_up_alarm) {
        /*
        "stamp,                state, state_code, info,                                      year, month, ip,           alarm_type"
        [2018-06-13 15:19:19.0,SOFT,  CRITICAL,   CRITICAL - Socket timeout after 10 seconds,2018, JUNE,  10.204.64.221,SERVICE]
         0                     1      2           3                                          4     5      6             7
         */
        String ip = not_up_alarm.get(6).toString();
        String key = "inventory:" + ip;
        Map<String, String> inventory = GetDataFromRedis(key);
        /*
        Jedis inventory:
        KEY:tipid,        VALUE:1
        KEY:vendor,       VALUE:fHiZtWUtVDA
        KEY:productnumber,VALUE:mI621zlP3
        KEY:silocationid, VALUE:976888
        KEY:dnsname,      VALUE:R-Zv6094HseRa-so357sdC3
        KEY:cisid,        VALUE:592461
        */
        StringBuilder pair = new StringBuilder();
        String hostname = inventory.get("dnsname");
        pair.append(hostname);
        pair.append(",");
        String cisid = inventory.get("cisid");
        pair.append(cisid);
        pair.append(",");
        String locationid = inventory.get("silocationid");
        pair.append(locationid);

        return pair.toString();
    }

    private Map<String, String> GetDataFromRedis(String key) {
        InstancePoolConfiguration redis_config = new InstancePoolConfiguration(SparkHiveAmalgamation.redis_host);
        InstancePool pool = InstancePool.POOL;
        try (Jedis redis_instance = pool.GetRedisInstance(redis_config)) {
            String inventory = redis_instance.get(key);
            Map<String, String> parsed_inventory = Parser.FlatJSONToMap(inventory);

            return parsed_inventory;
        }
    }

    private String GetPartitionData(Row alarm) {
        /*
        "stamp,                state, state_code, info,                                      year, month, ip,           alarm_type"
        [2018-06-13 15:19:19.0,SOFT,  CRITICAL,   CRITICAL - Socket timeout after 10 seconds,2018, JUNE,  10.204.64.221,SERVICE]
         0                     1      2           3                                          4     5      6             7
         */
        StringBuilder partions = new StringBuilder();
        partions.append(alarm.get(4).toString());
        partions.append(",");
        partions.append(alarm.get(5).toString());
        partions.append(",");
        partions.append(alarm.get(6).toString());
        partions.append(",");
        partions.append(alarm.get(7).toString());

        return partions.toString();
    }

    private int CalculateNumberOfPairs(List alarms_one, List alarms_two) {
        int length_one = alarms_one.size();
        int length_two = alarms_two.size();
        int pairs = Math.min(length_one, length_two);

        return pairs;
    }

    private String GetAlarmTimePair(Row not_up_alarm, Row up_alarm) {
        /*
        "stamp,                state, state_code, info,                                      year, month, ip,           alarm_type"
        [2018-06-13 15:19:19.0,SOFT,  CRITICAL,   CRITICAL - Socket timeout after 10 seconds,2018, JUNE,  10.204.64.221,SERVICE]
         0                     1      2           3                                          4     5      6             7
         */
        StringBuilder pair = new StringBuilder();
        String start_time = not_up_alarm.get(0).toString();
        pair.append(start_time);
        pair.append(",");
        String end_time = up_alarm.get(0).toString();
        pair.append(end_time);

        return pair.toString();
    }

    private String ConstructEnrichedRow(String time_pair, String other_data, String redis_inventory, String partitions) {
        StringBuilder enriched_row = new StringBuilder();
        enriched_row.append(time_pair);
        enriched_row.append(",");
        enriched_row.append(other_data);
        enriched_row.append(",");
        enriched_row.append(redis_inventory);
        enriched_row.append(",");
        enriched_row.append(partitions);

        return enriched_row.toString();
    }



    private void WriteIntoHive(List<String> enriched_alarms) {
        List<AlarmAggBean> alarms_agg = CreateAlarmsAgg(enriched_alarms);
        Encoder<AlarmAggBean> alarms_agg_encoder = Encoders.bean(AlarmAggBean.class);
        Dataset<AlarmAggBean> alarm_dataset = spark_session.createDataset(alarms_agg, alarms_agg_encoder);

        Dataset<Row> alarms_correctly_ordered = alarm_dataset.select("start_stamp", "end_stamp", "info", "service", "hostname", "cis_id", "location_id", "year", "month", "ip", "alarm_type"); //TODO get corect order from hive metastore

        alarms_correctly_ordered.write().mode(SaveMode.Append).insertInto("alarms_agg");
    }

    private List<AlarmAggBean> CreateAlarmsAgg(List<String> alarms) {
        ArrayList<AlarmAggBean> beaned_alarms = new ArrayList<>();
        AlarmAggBean bean;
        for (String alarm:alarms) {
            String[] alarm_elements = alarm.split(",");

            bean = new AlarmAggBean();
            bean.setStart_stamp(alarm_elements[0]);
            bean.setEnd_stamp(alarm_elements[1]);
            bean.setInfo(alarm_elements[2]);
            bean.setService(alarm_elements[3]);
            bean.setHostname(alarm_elements[4]);
            bean.setCis_id(alarm_elements[5]);
            bean.setLocation_id(alarm_elements[6]);
            bean.setYear(alarm_elements[7]);
            bean.setMonth(alarm_elements[8]);
            bean.setIp(alarm_elements[9]);
            bean.setAlarm_type(alarm_elements[10]);

            beaned_alarms.add(bean);
        }
        return beaned_alarms;
    }



    /**
     * You may want to use try-with-resources.
     */
    @Override
    public void close() {
        this.spark_session.close();
    }
}
