package mjaksic.from_hive_redis_to_spark_to_hive.closing;

import java.util.List;
import java.util.Map;

/**
 * Tracks data and implements conditions which will decide when a program should be closed gracefully.
 */
public class KafkaAndHiveCloser {
    private int empty_counter = 0;
    private int max_empty;

    /**
     *
     * @param config Config. See a link below.
     * @see bigdata.closing.KafkaAndHiveCloserConfiguration
     */
    public KafkaAndHiveCloser(KafkaAndHiveCloserConfiguration config) {
        SetConfig(config);
    }

    private void SetConfig(KafkaAndHiveCloserConfiguration config) {
        this.max_empty = config.max_empty;
    }

    public boolean IsStoppingConditionReached(List<Map<String, String>> map) {
        if (IsEmptyManyTimes(map)){
            return true;
        }
        return false;
    }

    private boolean IsEmptyManyTimes(List<Map<String, String>> map) {
        if (map.isEmpty()){
            empty_counter++;
            if (empty_counter > this.max_empty){
                return true;
            }
        } else {
            empty_counter = 0;
        };
        return false;
    }
}
