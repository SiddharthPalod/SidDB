package tx;

public class Snapshot {

    private final long sequenceNumber;

    public Snapshot(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String toString() {
        return "Snapshot[seq=" + sequenceNumber + "]";
    }
}
