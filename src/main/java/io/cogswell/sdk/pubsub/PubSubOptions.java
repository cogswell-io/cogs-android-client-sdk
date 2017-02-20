package io.cogswell.sdk.pubsub;

import android.net.Uri;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.cogswell.sdk.utils.Duration;

/**
 * Holds initialization options to use when first connect to Cogswell Pub/Sub
 */
public class PubSubOptions {

    /**
     * The default url to the production server.
     */
    private static final String PRODUCTION_URL = "wss://api.cogswell.io/pubsub";

    /**
     * The default timeout for connections.
     */
    private static final Duration DEFAULT_TIMEOUT = Duration.of(30, TimeUnit.SECONDS);

    /**
     * The url used for connecting to the Pub/Sub service
     */
    private Uri uri;

    /**
     * True if connection should auto-reconnect when dropped
     */
    private final boolean autoReconnect;

    /**
     * The amount of time, in milliseconds, when a connection attempt should timeout
     */
    private final Duration connectTimeout;

    /**
     * Holds UUID of the session to be restored if a session restore is requested.
     */
    private final UUID sessionUuid;

    /**
     * Initializes this PubSubOptions with all default values
     */
    public PubSubOptions () {
        this(PRODUCTION_URL, true, DEFAULT_TIMEOUT, null);
    }

    public PubSubOptions (String url) {
        this(url, true, DEFAULT_TIMEOUT, null);
    }

    /**
     * Static instance of PubSubOptions that contains all default values.
     */
    public static final PubSubOptions DEFAULT_OPTIONS = new PubSubOptions();

    /**
     * Initializes this PubSubOptions with the given options, filling in null values with defaults.
     *
     * @param url            URL to which to connect (Deafult: "wss://api.cogswell.io/pubsub").
     * @param autoReconnect  True if connection should attempt to reconnect when disconnected (Default: true).
     * @param connectTimeout Duration before connection should timeout (Default: 30 seconds).
     * @param sessionUuid    UUID of session to restore, if requested (Default: null).
     */
    public PubSubOptions(String url, Boolean autoReconnect, Duration connectTimeout, UUID sessionUuid) {
        this.uri = (url == null) ? Uri.parse(PRODUCTION_URL) : Uri.parse(url);
        this.autoReconnect = (autoReconnect == null) ? false : autoReconnect;
        this.connectTimeout = (connectTimeout == null) ? Duration.of(30, TimeUnit.SECONDS) : connectTimeout;
        this.sessionUuid = sessionUuid;
    }

    /**
     * Gets the url represented in this PubSubOptions for a connection.
     *
     * @return Uri The uri represented in this PubSubOptions instance.
     */
    public Uri getUri() {
        return uri;
    }

    /**
     * Gets whether these options represent the request to auto-reconnect.
     *
     * @return boolean True if auto-reconnect was set, false otherwise
     */
    public boolean getAutoReconnect() {
        return autoReconnect;
    }

    /**
     * Gets the time, in milliseconds, before a connection attempt should fail.
     * @return long Time, in milliseconds, before connection attempt should fail.
     */
    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Gets the UUID of the session requested to be re-established using this PubSubOptions instance.
     * @return UUID UUID of session requested to be re-established.
     */
    public UUID getSessionUuid() {
        return sessionUuid;
    }
}
