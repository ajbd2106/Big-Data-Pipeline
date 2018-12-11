package mjaksic.from_hive_to_kafka.hive.streaming;

/**
 * Convenience class. Carries both string and byte data.
 */
public class Record {
    public String string_record;
    public byte[] byte_record;

    public Record(String record) {
        this.string_record = record;
        this.byte_record = TransformStringToBytes(record);
    }

    private byte[] TransformStringToBytes(String string){
        return string.getBytes();
    }
}