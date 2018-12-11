package mjaksic.from_hive_redis_to_spark_to_hive.spark;

import java.io.Serializable;

/**
 * Hive table bean.
 * In short, a bean must have; an empty Constructor; all fields set to private; setters and getters for all fields.
 * @see <a href="http://www.oracle.com/technetwork/java/javase/documentation/spec-136004.html">Java Bean Standard</a>
 */
public class AlarmAggBean implements Serializable { //TODO couple Hive table and the bean; SRP, if the table changes, the bean should change as well
	private static final long serialVersionUID = 4521346476754348757L;
	
	private String start_stamp;
    private String end_stamp;
    private String info;
    private String service;
    private String hostname;
    private String cis_id;
    private String location_id;
    private String year;
    private String month;
    private String ip;
    private String alarm_type;

    public AlarmAggBean() {

    }

    public String getStart_stamp() {
        return start_stamp;
    }

    public void setStart_stamp(String start_stamp) {
        this.start_stamp = start_stamp;
    }

    public String getEnd_stamp() {
        return end_stamp;
    }

    public void setEnd_stamp(String end_stamp) {
        this.end_stamp = end_stamp;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getCis_id() {
        return cis_id;
    }

    public void setCis_id(String cis_id) {
        this.cis_id = cis_id;
    }

    public String getLocation_id() {
        return location_id;
    }

    public void setLocation_id(String location_id) {
        this.location_id = location_id;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getAlarm_type() {
        return alarm_type;
    }

    public void setAlarm_type(String alarm_type) {
        this.alarm_type = alarm_type;
    }
}