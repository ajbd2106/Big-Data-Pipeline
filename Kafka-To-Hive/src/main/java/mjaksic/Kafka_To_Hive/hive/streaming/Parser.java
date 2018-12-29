package mjaksic.Kafka_To_Hive.hive.streaming;

import java.time.Month;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Parses and extracts values from a data structure (collection) and constructs another data structure (collection) using parsed values.
 */
public class Parser { //TODO decouple table column name ordering from this whole class; use the metastore client to get the ordering
    public static String default_sentinel = "NONE";

    private StringBuilder construct;

    public Parser() {

    }

    private void SetStringBuilder(){
        this.construct = new StringBuilder();
    }

    public void ResetStringBuilder() {
        SetStringBuilder();
    }



    /**
     *
     * @param map Transform :
     *              {"start_time": "2018/06/13 15:11:10",
     *              "state": "SOFT",
     *              "status_code": "DOWN",
     *              "eventname": "HOST ALERT",
     *              "info": "Modem ...",
     *              "ipadresa": "10.204.78.230",
     *              "end_time": "2018/06/13 15:16:11"}
     * @return ...into two messages:
     *              2018/06/13 15:11:10,
     *              SOFT,
     *              DOWN,
     *              1,
     *              HOST,
     *              Modem ...,
     *              2018,
     *              JUNE,
     *              10.204.78.230
     *
     *              2018/06/13 15:16:11,
     *              HARD,
     *              UP,
     *              0,
     *              HOST,
     *              Modem ...,
     *              2018,
     *              JUNE,
     *              10.204.78.230
     *
     * If a Map is missing data, the resulting strings will have a sentinel value.
     *
     */
    public List<String> AlarmsRawToCSV(Map<String, String> map){
        ResetStringBuilder();
        String start_time_alarm_message = GetStartTimeAlarmMessage(map);
        ResetStringBuilder();
        String end_time_alarm_message = GetEndTimeAlarmMessage(map);

        ArrayList<String> messages = new ArrayList<>();
        messages.add(start_time_alarm_message);
        messages.add(end_time_alarm_message);

        return messages;
    }



    private void AddCSVStringToBuilder(String string){
        this.construct.append(string);
        this.construct.append(',');
    }

    private void DiscardLastCommaFromBuilder() {
        DiscardLastCharFromBuilder();
    }

    private void DiscardLastCharFromBuilder() {
        String string = this.construct.substring(0,this.construct.length()-1);
        ResetStringBuilder();
        this.construct.append(string);
    }


    private String GetStartTimeAlarmMessage(Map<String, String> data){
        AddCSVStringToBuilder(ParseStartTime(data));
        AddStartTimeAlarmsColumnData(data);
        AddAlarmsPartitionData(data);
        DiscardLastCommaFromBuilder();

        return construct.toString();
    }

    private String GetEndTimeAlarmMessage(Map<String, String> data) {
        AddCSVStringToBuilder(ParseEndTime(data));
        AddEndTimeAlarmsColumnData(data);
        AddAlarmsPartitionData(data);
        DiscardLastCommaFromBuilder();

        return construct.toString();
    }

    private String ParseStartTime(Map<String, String> data){
        String raw_end_time = data.getOrDefault("start_time", getDefault_sentinel());
        String hive_timestamp_string = TransformStringIntoHiveTimestampFormat(raw_end_time);
        return hive_timestamp_string;
    }

    private String ParseEndTime(Map<String, String> data){
        String raw_end_time = data.getOrDefault("end_time", getDefault_sentinel());
        String hive_timestamp_string = TransformStringIntoHiveTimestampFormat(raw_end_time);
        return hive_timestamp_string;
    }

    private String TransformStringIntoHiveTimestampFormat(String raw_string){
        String hive_timestamp_string = raw_string.replace("/", "-");
        return hive_timestamp_string;
    }


    private void AddStartTimeAlarmsColumnData(Map<String, String> data) {
        AddCSVStringToBuilder(ParseState(data));
        AddCSVStringToBuilder(ParseStateCode(data));
        AddCSVStringToBuilder(ParseStatus(data));

        AddCSVStringToBuilder(ParseAlarmType(data));
        AddCSVStringToBuilder(ParseInfo(data));
    }

    private void AddEndTimeAlarmsColumnData(Map<String, String> data) {
        AddCSVStringToBuilder(ParseSetState(data));
        AddCSVStringToBuilder(SetStateCodeUp(data));
        AddCSVStringToBuilder(SetStatusZero(data));

        AddCSVStringToBuilder(ParseAlarmType(data));
        AddCSVStringToBuilder(ParseInfo(data));
    }

    private String ParseState(Map<String, String> data){
        return data.getOrDefault("state", getDefault_sentinel());
    }

    private String ParseSetState(Map<String, String> data){
        return "HARD";
    }

    private String ParseStateCode(Map<String, String> data){
        return data.getOrDefault("status_code", getDefault_sentinel());
    }

    private String SetStateCodeUp(Map<String, String> data){
        return "UP";
    }

    private String ParseStatus(Map<String, String> data){
        //UP == 0
        //DOWN == 1
        //WARNING == 2
        //CRITICAL == 3
        String status_code = data.getOrDefault("status_code", getDefault_sentinel());
        String status;
        if (status_code.equals("UP")){
            status = "0";
        } else if (status_code.equals("DOWN")) {
            status = "1";
        }else if (status_code.equals("WARNING")) {
            status = "2";
        }else if (status_code.equals("CRITICAL")) {
            status = "3";
        } else {
            status = "7"; //7 for exception
        }
        return status;
    }

    private String SetStatusZero(Map<String, String> data){
        //UP == 0
        //DOWN == 1
        //WARNING == 2
        //CRITICAL == 3
        return "0";
    }

    private String ParseAlarmType(Map<String, String> data){
        String raw_alarm_type = data.getOrDefault("eventname", getDefault_sentinel());
        String[] split_string = raw_alarm_type.split(" ");
        return split_string[0];
    }

    private String ParseInfo(Map<String, String> data){
        return data.getOrDefault("info", getDefault_sentinel());
    }


    private void AddAlarmsPartitionData(Map<String, String> data) {
        AddCSVStringToBuilder(ParseYear(data));
        AddCSVStringToBuilder(ParseMonth(data));
        AddCSVStringToBuilder(ParseIP(data));
    }

    private String ParseYear(Map<String, String> data) {
        String raw_date = data.getOrDefault("start_time", getDefault_sentinel());
        String[] split_date = raw_date.split("/");
        String year = split_date[0];
        return year;
    }

    private String ParseMonth(Map<String, String> data) {
        String raw_date = data.getOrDefault("start_time", getDefault_sentinel());
        String[] split_date = raw_date.split("/");
        int month_number = Integer.parseInt(split_date[1]);
        String month = Month.of(month_number).toString();
        return month;
    }

    private String ParseIP(Map<String, String> data) {
        return data.getOrDefault("ipadresa", getDefault_sentinel());
    }


    public static String getDefault_sentinel() {
        return default_sentinel;
    }
}
