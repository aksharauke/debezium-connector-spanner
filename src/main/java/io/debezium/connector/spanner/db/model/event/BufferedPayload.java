package io.debezium.connector.spanner.db.model.event;

/** */
public class BufferedPayload {

    private long commitTs;

    private String trxId;

    private long recSeq;

    private long numRec;

    private String payload;

    public BufferedPayload(long cTs, String tx, long rec, long nRec, String data) {
        this.commitTs = cTs;
        this.trxId = tx;
        this.recSeq = rec;
        this.numRec = nRec;
        this.payload = data;
    }

    @Override
    public String toString() {

        return "CommitTs: "
                + this.commitTs
                + " TrxId: "
                + this.trxId
                + " RecSeq: "
                + this.recSeq
                + " NumRec: "
                + this.numRec
                + "  Payload: "
                + this.payload;
    }
}
