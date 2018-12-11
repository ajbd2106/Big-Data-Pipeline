package mjaksic.from_hive_redis_to_spark_to_hive.sql;

public class SelectConfiguration {
    public String select;
    public String from;
    public String where;
    public String group_by;
    public String order_by;
    public String limit;

    public SelectConfiguration(String select, String from, String where, String group_by, String order_by, String limit) {
        this.select = select;
        this.from = from;
        this.where = where;
        this.group_by = group_by;
        this.order_by = order_by;
        this.limit = limit;
    }
}
