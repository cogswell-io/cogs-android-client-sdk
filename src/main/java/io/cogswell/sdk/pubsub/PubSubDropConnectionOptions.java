package io.cogswell.sdk.pubsub;

import io.cogswell.sdk.utils.Duration;

/**
 * Created by dlewton on 2/17/17.
 */

public class PubSubDropConnectionOptions {
    /**
     * Holds the shortest initial time to wait before attempting a reconnect.
     * This overwrites the current initial time, which is the reset to minimum default.
     */
    private final Duration autoReconnectDelay;

    /**
     * Initializes options for dropping a connection.
     * @param autoReconnectDelay Minimum time to wait before reconnect.
     */
    public PubSubDropConnectionOptions(Duration autoReconnectDelay) {
        this.autoReconnectDelay = autoReconnectDelay;
    }

    public Duration getReconnectDelay() {
        return autoReconnectDelay;
    }
}
