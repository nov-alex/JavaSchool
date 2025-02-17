package sbp.school.kafka.entity;

import java.time.LocalDateTime;
import java.util.Arrays;

public record AckData(LocalDateTime start,
                      LocalDateTime end,
                      byte[] hash) {

    @Override
    public String toString() {
        return "AckData{" +
                "start=" + start +
                ", end=" + end +
                ", hash='" + Arrays.toString(hash) + '\'' +
                '}';
    }
}