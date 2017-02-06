package io.cogswell.sdk.pubsub.exceptions;

/**
 * Represents an Exception generated by the Pub/Sub SDK.
 */
public class PubSubException extends Exception {
    /**
     * Creates an exception without a message.
     */
    public PubSubException() {
        super();
    }

    /**
     * Creates an exceptoin with the message given in {@code message}
     *
     * @param message Message that the PubSubException contains.
     */
    public PubSubException(String message) {
        super(message);
    }

    /**
     * Creates an exceptoin with the message given in {@code message}
     *
     * @param message Message that the PubSubException contains.
     * @param cause The source of this exception.
     */
    public PubSubException(String message, Throwable cause) {
        super(message, cause);
    }
}