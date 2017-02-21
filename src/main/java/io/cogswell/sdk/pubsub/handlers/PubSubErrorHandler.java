package io.cogswell.sdk.pubsub.handlers;

/**
 * Represents an error handler for when the underlying socket errors.
 */
public interface PubSubErrorHandler {
    /**
     * Invoked when an error occurs within the handler. This could be an issue with the
     * socket, an unexpected response from the server, or any other client-side failure.
     *
     * @param error     Error that occurred which caused the invocation of the handler
     */
    void onError(Throwable error);
}