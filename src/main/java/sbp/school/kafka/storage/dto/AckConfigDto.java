package sbp.school.kafka.storage.dto;

import java.time.Duration;

public class AckConfigDto {

    private final Duration timeSliceSec;
    private final Duration timeDelaySec;

    public AckConfigDto(Duration timeSlice, Duration timeDelay) {
        this.timeSliceSec = timeSlice;
        this.timeDelaySec = timeDelay;
    }

    public Duration getTimeSliceSec() {
        return timeSliceSec;
    }

    public Duration getTimeDelaySec() {
        return timeDelaySec;
    }
}
