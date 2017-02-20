package io.cogswell.sdk.pubsub;

/**
 * Created by dlewton on 2/17/17.
 */

public class PubSubDropConnectionOptions {
    /**
     * Holds the shortest initial time to wait before attempting a reconnect.
     * This overwrites the current initial time, which is the reset to minimum default.
     */
    private final long autoReconnectDelay;

    /**
     * Initializes options for dropping a connection.
     * @param autoReconnectDelay Minimum time to wait before reconnect.
     */
    public PubSubDropConnectionOptions(long autoReconnectDelay) {
        this.autoReconnectDelay = autoReconnectDelay;
    }

    public long getReconnectDelay() {
        return autoReconnectDelay;
    }
}
