package io.cogswell.sdk.pubsub.handlers;

/**
 * Represents an error handler for when the underlying socket errors.
 */
public interface PubSubErrorResponseHandler {
    /**
     * Invoked any time a valid error response is received from the server.
     *
     * @param sequence Sequence of the request to which the server is responding.
     * @param action   Action of the request to which the server is responding.
     * @param code     Response code from the server.
     * @param channel  Channel (if any) to which the originating message/request referred.
     */
    void onError(Long sequence, String action, Integer code, String channel);
}