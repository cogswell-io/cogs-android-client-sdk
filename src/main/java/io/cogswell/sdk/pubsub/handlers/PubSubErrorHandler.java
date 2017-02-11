package io.cogswell.sdk.pubsub.handlers;

/**
 * Represents an error handler for when the underlying socket errors.
 */
public interface PubSubErrorHandler {
    /**
     * Invoked when an error occurs with the socket underlying an instance {@link io.cogswell.sdk.pubsub.PubSubSocket}
     *
     * @param error     Error that occurred which caused the invocation of the handler
     * @param sequence Sequence of the message/request related to the error that occurred.
     * @param channel  Channel of which the message/request related to the error is associated.
     */
    void onError(Throwable error, Long sequence, String channel);
}