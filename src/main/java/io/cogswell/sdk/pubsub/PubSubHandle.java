package io.cogswell.sdk.pubsub;

import android.util.Log;

import java.util.concurrent.atomic.AtomicLong;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.base.Function;

import io.cogswell.sdk.pubsub.handlers.*;

/**
 * Represents user endpoint to Cogswell Pub/Sub and provides methods to perform available Pub/Sub operations.
 */
public class PubSubHandle {
    private AtomicLong sequence;
    private PubSubSocket socket;

    /**
     * Creates an endpoint to Cogswell Pub/Sub using the given {@link PubSubSocket} as the underlying connection.
     *
     * @param socket {@link PubSubSocket} which contains the underlying connection to Cogswell Pub/Sub
     * @param initialSequenceNumber The initial sequence number to be used.
     */
    protected PubSubHandle(PubSubSocket socket, Long initialSequenceNumber) {
        this.sequence = new AtomicLong(initialSequenceNumber);
        this.socket = socket;
    }

    private interface JSONReaderFunction<T> {
        T apply(JSONObject json) throws JSONException;    };

    /**
     * Send a message with the specified params and process the JSON response with the specified reader.
     * A sequence number will be added to the request.  Any errors will resolve the Future with an exception.
     *
     * @param jsonreader converts a JSONObject to an instance of the specified type.
     * @param nameValuePairs Must follow the pattern of a string name followed by a value (String, Object, String, Object, ...)
     * @param isAwaitingServerResponse If true, this call will be registered to resolve when the server responds.  Otherwise it will resolve immediatly with a sequence number.
     * @param <T> The expected result type.
     *
     * @return A future with the result of a call to jsonreader.
     */
    private <T> ListenableFuture<T> sendJSON(
            final JSONReaderFunction<T> jsonreader, final boolean isAwaitingServerResponse,
            final PubSubErrorHandler serverErrorHandler, Object ... nameValuePairs
    ) {
        final SettableFuture<T> outcome = SettableFuture.create();
        long seq = sequence.getAndIncrement();

        try {
            JSONObject request = new JSONObject()
                    .put("seq", seq);

            for(int i = 0; i < nameValuePairs.length-1; i += 2) {
                request.put((String)nameValuePairs[i], nameValuePairs[i+1]);
            }

            ListenableFuture<JSONObject> getSessionFuture = socket.sendRequest(seq, request, isAwaitingServerResponse, serverErrorHandler);

            Futures.addCallback(getSessionFuture, new FutureCallback<JSONObject>() {
                public void onSuccess(JSONObject json) {
                    try {
                        T result = jsonreader.apply(json);
                        outcome.set(result);
                    } catch (JSONException e) {
                        Log.e("COGS-SDK", "Server sent unexpected response" + json.toString(), e);
                        outcome.setException(e);
                    }
                }
                public void onFailure(Throwable t) {
                    Log.e("TEST","Error:",t);
                    outcome.setException(t);
                }
            });
        } catch (JSONException e) {
            // Reject the future if there are any errors.
            outcome.setException(e);
        }

        return outcome;
    }

    /**
     * Fetches UUID of current session, which enables caching if caching is enabled on the project.
     * @return ListenableFuture<UUID> Future that completes with Session UUID on success, and with error otherwise
     */
    public ListenableFuture<UUID> getSessionUuid() {
        return sendJSON(new JSONReaderFunction<UUID>() {
            @Override
            public UUID apply(JSONObject json) throws JSONException {
                return UUID.fromString(json.getString("uuid"));
            }
        }, true, null, "action", "session-uuid");
    }

    /**
     * Subscribe to the given {@code channel}, and process messages from {@code channel} using the provided {@link PubSubMessageHandler}.
     *
     * @param channel The name of the channel to which to subscribe.
     * @param messageHandler Handler that receives message from {@code channel}. May NOT be null.
     * @return ListenableFuture<List<String>> Completes with list of all current subscriptions on success.
     */
    public ListenableFuture<List<String>> subscribe(String channel, PubSubMessageHandler messageHandler) {
        socket.setMessageHandler(channel, messageHandler);

        return sendJSON(new JSONReaderFunction<List<String>>() {
            @Override
            public List<String> apply(JSONObject json) throws JSONException {
                List<String> channels = Collections.synchronizedList(new LinkedList<String>());
                JSONArray list = json.getJSONArray("channels");

                for(int i = 0; i < list.length(); ++i) {
                    channels.add(list.getString(i));
                }
                return channels;
            }
        }, true, null, "action", "subscribe", "channel", channel);
    }

    /**
     * Unsubscribes from {@code channel} which stops receipt and handling of messages for {@code channel}.
     *
     * @param channel Name of the channel from which to unsubscribe.
     * @return ListenableFuture<List<String>> Completes with list of all remaining subscriptions on success.
     */
    public ListenableFuture<List<String>> unsubscribe(final String channel) {
        return sendJSON(new JSONReaderFunction<List<String>>() {
            @Override
            public List<String> apply(JSONObject json) throws JSONException {
                List<String> channels = Collections.synchronizedList(new LinkedList<String>());
                JSONArray list = json.getJSONArray("channels");

                for(int i = 0; i < list.length(); ++i) {
                    channels.add(list.getString(i));
                }

                socket.removeMessageHandler(channel);

                return channels;
            }
        }, true, null, "action", "unsubscribe", "channel", channel);
    }
    
    /**
     * Unsubscribes from all channels.   This will stop receipt and handling of messages from all channels.
     * @return ListenableFuture<List<String>> Completes with list of all unsubscribed channels.
     */
    public ListenableFuture<List<String>> unsubscribeAll() {
        return sendJSON(new JSONReaderFunction<List<String>>() {
            @Override
            public List<String> apply(JSONObject json) throws JSONException {
                List<String> channels = Collections.synchronizedList(new LinkedList<String>());
                JSONArray list = json.getJSONArray("channels");

                for(int i = 0; i < list.length(); ++i) {
                    channels.add(list.getString(i));
                }

                socket.removeAllMessageHandlers();

                return channels;
            }
        }, true, null, "action", "unsubscribe-all");
    }

    /**
     * Fetches list of all current subscriptions.
     * @return ListenableFuture<List<String>> Completes with list of all current subscriptions on success.
     */
    public ListenableFuture<List<String>> listSubscriptions() {
        return sendJSON(new JSONReaderFunction<List<String>>() {
            @Override
            public List<String> apply(JSONObject json) throws JSONException {
                List<String> channels = Collections.synchronizedList(new LinkedList<String>());
                JSONArray list = json.getJSONArray("channels");

                for(int i = 0; i < list.length(); ++i) {
                    channels.add(list.getString(i));
                }

                return channels;
            }
        }, true, null, "action", "subscriptions");
    }

    /**
     * Publishes {@code message} to {@code channel} without acknowledgement that the message was
     * actually published.
     *
     * Note: Completion of the returned CompletableFuture indicates success only in delivery of the
     * message to the server. This method gives no information and no guarantees that the message
     * was actually published by the server.
     *
     * @param channel Name of the channel on which to publish the message.
     * @param message Content of the message to be publish on the given channel.
     *
     * @return ListenableFuture<Long> Completes with the sequence number of the request on a
     * successful send.
     */
    public ListenableFuture<Long> publish(String channel, String message) {
        return publish(channel, message, null);
    }

    /**
     * Publishes {@code message} to {@code channel} without acknowledgement that the message was
     * actually published.
     *
     * Note: Completion of the returned CompletableFuture indicates success only in delivery of the
     * message to the server. This method gives no information and no guarantees that the message
     * was actually published by the server.
     *
     * @param channel Name of the channel on which to publish the message.
     * @param message Content of the message to be publish on the given channel.
     * @param handler Error handler called if the server reports an error within 30 seconds
     *                of sending.
     *
     * @return ListenableFuture<Long> Completes with the sequence number of the request on a
     * successful send.
     */
    public ListenableFuture<Long> publish(String channel, String message, PubSubErrorHandler handler) {
        return sendJSON(new JSONReaderFunction<Long>() {
            @Override
            public Long apply(JSONObject json) throws JSONException {
                Long seq = json.getLong("seq");
                return seq;
            }
        }, false, handler, "action", "pub", "chan", channel, "msg", message, "ack", false);
    }

    /**
     * Publishes {@code message} to {@code channel} with acknowledgement that the message was
     * actually published by the server.
     *
     * @param channel Name of the channel on which to publish the message.
     * @param message Content of the message to be publish on the given channel.
     *
     * @return ListenableFuture<Long> Completes with UUID of published message on success.
     */
    public ListenableFuture<UUID> publishWithAck(String channel, String message) {
        return sendJSON(new JSONReaderFunction<UUID>() {
            @Override
                public UUID apply(JSONObject json) throws JSONException {
                UUID uuid = UUID.fromString(json.getString("id"));
                return uuid;
            }
        }, true, null, "action", "pub", "chan", channel, "msg", message, "ack", true);
    }

    /**
     * Closes the connection with Cogswell Pub/Sub and unsubscribes from all channels.
     *
     * @return ListenableFuture<List<String>> which, on success, completes with the list of channels
     * from which the session was unsubscribed.
     */
    public ListenableFuture<List<String>> close() {
        ListenableFuture<List<String>> unsubscribeAllFuture = unsubscribeAll();

        Function<List<String>, List<String>> closeFunction = new Function<List<String>, List<String>>() {
            public List<String> apply(List<String> unsubscribeAllResponse) {
                socket.close();
                return unsubscribeAllResponse;
            }
        };

        ListenableFuture<List<String>> closedFuture = Futures.transform(unsubscribeAllFuture, closeFunction);

        return closedFuture;
    }

    /**
     * Drop the underlying socket.  If auto-reconnect is enabled, the underlying socked will
     * be replaced.
     * @param options Options to fine-tune the effect of dropping the connection
     */
    public void dropConnection(PubSubDropConnectionOptions options) {
        socket.dropConnection(options);
    }

    /**
     * Registers a handler to process any published messages received from Cogswell Pub/Sub on any subscribed channels.
     *
     * @param messageHandler The {@link PubSubMessageHandler} that should be registered.
     */
    public void onMessage(PubSubMessageHandler messageHandler) {
        socket.setMessageHandler(messageHandler);
    }

    /**
     * Registers a handler that is called whenever the underlying connection is re-established.
     *
     * @param reconnectHandler The {@link PubSubReconnectHandler} that should be registered.
     */
    public void onReconnect(PubSubReconnectHandler reconnectHandler) {
        socket.setReconnectHandler(reconnectHandler);
    }

    /**
     * Registers a handler to process every raw record (as a JSON-formatted String) received from Cogswell Pub/Sub.
     *
     * @param rawRecordHandler The {@link PubSubRawRecordHandler} that should be registered.
     */
    public void onRawRecord(PubSubRawRecordHandler rawRecordHandler) {
        socket.setRawRecordHandler(rawRecordHandler);
    }

    /**
     * Registers a handler that is called immediately before the underlying connection to Cogswell Pub/Sub is closed.
     *
     * @param closeHandler The {@link PubSubCloseHandler} that should be registered.
     */
    public void onClose(PubSubCloseHandler closeHandler) {
        socket.setCloseHandler(closeHandler);
    }

    /**
     * Registers a handler that is called whenever there is any error with the underlying connection to Cogswell Pub/Sub
     *
     * @param errorHandler The {@link PubSubErrorHandler} that should be registered
     */
    public void onError(PubSubErrorHandler errorHandler) {
        socket.setErrorHandler(errorHandler);
    }

    // TODO: [PUB-318] add the onErrorResponse() method

    /**
     * Registers a handler that is called whenever reconnecting the underlying connection to Cogswell Pub/Sub forces a new session
     *
     * @param newSessionHandler The {@link PubSubNewSessionHandler} that should be registered
     */
    public void onNewSession(PubSubNewSessionHandler newSessionHandler) {
        socket.setNewSessionHandler(newSessionHandler);
    }
}