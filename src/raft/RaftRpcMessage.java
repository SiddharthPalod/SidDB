package raft;

import java.io.Serializable;

/**
 * Base abstract class for all Raft RPC requests and responses.
 * Encapsulates the logical clock 'term' common to all Raft communication.
 */
public abstract class RaftRpcMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final long term;

    public RaftRpcMessage(long term) {
        this.term = term;
    }

    public long getTerm() {
        return term;
    }
}
