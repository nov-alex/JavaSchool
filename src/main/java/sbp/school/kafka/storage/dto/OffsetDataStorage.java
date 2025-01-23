package sbp.school.kafka.storage.dto;

/**
 * Хранилище для смещений
 */
public class OffsetDataStorage {
    private long commitedOffset;
    private long uncommitedOffset;

    public OffsetDataStorage(long commitedOffset, long uncommitedOffset) {
        this.commitedOffset = commitedOffset;
        this.uncommitedOffset = uncommitedOffset;
    }

    public long getCommitedOffset() {
        return commitedOffset;
    }

    public OffsetDataStorage setCommitedOffset(long commitedOffset) {
        this.commitedOffset = commitedOffset;
        if (commitedOffset > uncommitedOffset) {
            uncommitedOffset = commitedOffset;
        }
        return this;
    }

    public long getUncommitedOffset() {
        return uncommitedOffset;
    }

    public OffsetDataStorage setUncommitedOffset(long uncommitedOffset) {
        this.uncommitedOffset = uncommitedOffset;
        return this;
    }

    public void commit() {
        this.commitedOffset = uncommitedOffset;
    }
}
