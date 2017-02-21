package io.cogswell.sdk.pubsub;

import android.util.Log;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.koushikdutta.async.callback.CompletedCallback;
import com.koushikdutta.async.http.AsyncHttpClient;
import com.koushikdutta.async.http.AsyncHttpRequest;
import com.koushikdutta.async.http.Headers;
import com.koushikdutta.async.http.WebSocket;

import java.sql.Time;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;
import java.util.List;

import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;

import io.cogswell.sdk.Auth;
import io.cogswell.sdk.json.Json;
import io.cogswell.sdk.json.JsonNode;
import io.cogswell.sdk.json.JsonObject;
import io.cogswell.sdk.pubsub.exceptions.*;
import io.cogswell.sdk.pubsub.handlers.*;

/**
 * Wraps the logic of {@link com.koushikdutta.async.http.WebSocket} websockets.
 * It also tracks and routes both incoming and outgoing message to and from Cogswell Pub/Sub.
 */
public class PubSubSocket implements WebSocket.StringCallback, AsyncHttpClient.WebSocketConnectCallback, CompletedCallback
{
    public static ListenableFuture<PubSubSocket> connectSocket(List<String> projectKeys)
    {
        return connectSocket(projectKeys, PubSubOptions.DEFAULT_OPTIONS, MoreExecutors.directExecutor());
    }

    public static ListenableFuture<PubSubSocket> connectSocket(List<String> projectKeys, PubSubOptions options)
    {
        return connectSocket(projectKeys, options, MoreExecutors.directExecutor());
    }

    public static ListenableFuture<PubSubSocket> connectSocket(List<String> projectKeys, PubSubOptions options, Executor executor)
    {
        try {
            PubSubSocket socket = new PubSubSocket(projectKeys, options, executor);
            return socket.connect();
        } catch (Auth.AuthKeyError e) {
            SettableFuture<PubSubSocket> result = SettableFuture.create();
            result.setException(e);
            return result;
        }
    }

    /**
     * This controls the threading policy for all async tasks.
     */
    private Executor executor;

    /**
     * This manages the keep-alive thread.
     */
    private ScheduledExecutorService keepAliveExecutorSerivce;

    /**
     * This is the keep-alive task, which pings the web-socket.
     */
    private Runnable keepAliveRunnable;

    /**
     * This is the interval at which the keep-alive task is executed.
     */
    private long keepAliveIntervalMs;

    /**
     * This is a reference to the execution of the keep-alive task, used for canceling the task.
     */
    private ScheduledFuture keepAliveFuture;

    /**
     * Handler called when keep-alive pings are sent.  This is protected for testing.
     */
    private Runnable keepAliveHandler;

    /**
     * This is the shortest default delay that the socket will wait between reconnects
     * Current it is set to 5 seconds = 5 s * 1000 ms/s = 5000 ms.
     */
    private static final long MIN_RECONNECT_DELAY_MILLIS = 5000L; // 5 seconds

    /**
     * This is the longest the socket will wait between reconnects before giving up.
     * Currently it is set to 2 minutes = 2 min * 60 s/min * 1000 ms/s = 120000 ms.
     */
    private static final long MAX_RECONNECT_DELAY_MILLIS = 120000L; // 2 minutes

    /**
     * The keys used when creating this PubSubSocket.
     */
    private List<String> projectKeys;

    /**
     * The {@link PubSubOptions} used when creating this PubSubSocket.
     */
    private PubSubOptions options;

    /**
     * The {@link WebSocket} that represents this PubSubSocket as a websocket Endpoint connection.
     */
    WebSocket webSocketSession;

    /**
     * Tracks whether this socket is actually connected to the Pub/Sub server
     */
    private AtomicBoolean isConnected;

    /**
     * Tracks whether this socket is actually connected to the Pub/Sub server
     */
    private AtomicBoolean isSetupInProgress;

    /**
     * This future is resolved when the socket is connected.
     */
    private SettableFuture<PubSubSocket> setupFuture;

    /**
     * Holds whether to actually reconnect (based on length of delay, and whether close was chosen)
     */
    private AtomicBoolean autoReconnect;

    /**
     * Holds the next delay to wait when an attempted reconnect fails
     */
    private AtomicLong autoReconnectDelayMillis;

    /**
     * Holds the current session uuid from the Pub/Sub server
     */
    private UUID sessionUuid;

    /**
     * Holds whether the most recent session UUID meant that a new session was generated
     */
    private AtomicBoolean isNewSession;

    /**
     * Maps each outstanding request to the server by their sequence number 
     * with their associated {@link com.google.common.util.concurrent.ListenableFuture}
     */
    private Cache<Long, SettableFuture<JSONObject>> outstanding;

    /**
     * Maps outstanding publish request sequence numbers to their server error handlers.
     */
    private Cache<Long, PubSubErrorResponseHandler> publishErrorHandlers;

    /**
     * Maps outstanding publish request sequence numbers to their request objects.
     */
    private Cache<Long, JSONObject> publishErrorHandlerRequests;

    /**
     * Maps the channel subscriptions of this PubSubSocket with the specific message handlers given for those channels. 
     */
    private Map<String, PubSubMessageHandler> msgHandlers;

    /**
     * Handler called whenever server generates a new session for this connection
     */
    private PubSubNewSessionHandler newSessionHandler;

    /**
     * Handler called whenever this connection must reconnect for some reason
     */
    private PubSubReconnectHandler reconnectHandler;

    /**
     * Handler called whenever any raw string json message is received from the server
     */
    private PubSubRawRecordHandler rawRecordHandler;

    /**
     * Handler called as general message handler whenever published messages are received from server 
     */
    private PubSubMessageHandler generalMsgHandler;

    /**
     * Handler called whenever an error having to do with this connection is encountered
     */
    private PubSubErrorHandler errorHandler;

    /**
     * Handler called whenever an error response is received from the server
     */
    private PubSubErrorResponseHandler errorResponseHandler;

    /**
     * Handler called whenever this connection closes
     */
    private PubSubCloseHandler closeHandler; 

    /**
     * Creates a minimal PubSubSocket, used for testing purposes
     */
    protected PubSubSocket() {
        this(null, PubSubOptions.DEFAULT_OPTIONS, MoreExecutors.directExecutor());
        this.autoReconnectDelayMillis = new AtomicLong(MIN_RECONNECT_DELAY_MILLIS);
    }

    /**
     * Creates a minimal PubSubSocket, using the provided server as the connect to which to send messages
     * Used for testing purposes
     * @param webSocketSession The server to which to send messages
     */
    protected PubSubSocket(WebSocket webSocketSession) {
        this();
        this.webSocketSession = webSocketSession;
    }

    /**
     * Creates a connection to the Pub/Sub server given project keys and {@link PubSubOptions}
     * @param projectKeys The configuration requested for the connection represented by this PubSubSocket
     * @param options The options requested for the connection represented by this PubSubSocket
     * @throws IOException
     */
    public PubSubSocket(List<String> projectKeys, PubSubOptions options, Executor executor)
    {
        this.projectKeys = projectKeys;
        this.options = options;
        this.executor = executor;

        this.sessionUuid = options.getSessionUuid();
        this.autoReconnectDelayMillis = new AtomicLong(Math.max(options.getConnectTimeout().millis(), MIN_RECONNECT_DELAY_MILLIS));
        this.autoReconnect = new AtomicBoolean(options.getAutoReconnect());

        this.isConnected = new AtomicBoolean(false);
        this.isSetupInProgress = new AtomicBoolean(false);
        this.setupFuture = null;

        this.msgHandlers = Collections.synchronizedMap(new Hashtable<String, PubSubMessageHandler>());
        this.outstanding = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS).build();
        this.publishErrorHandlers = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS).build();
        this.publishErrorHandlerRequests = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS).build();

        this.keepAliveExecutorSerivce = Executors.newSingleThreadScheduledExecutor();
        this.keepAliveRunnable = new Runnable() {
            @Override
            public void run() {
                if (PubSubSocket.this.isConnected.get() && webSocketSession !=null && webSocketSession.isOpen() && !webSocketSession.isPaused()) {
                    if (keepAliveHandler != null) {
                        keepAliveHandler.run();
                    }
                    webSocketSession.ping("");
                }
            }
        };
        this.keepAliveIntervalMs = 30000;

    }

    /**
     * Closes the connection represented by this PubSubSocket
     */
    public void close() 
    {
        autoReconnect.set(false);
        webSocketSession.close();
    }

    /**
     * Sends the given request, represented by the {@link org.json.JSONObject}, to the server and maps the
     * eventual result to be stored in a {@link com.google.common.util.concurrent.ListenableFuture} with the sequence
     * number of the message.
     * @param sequence Sequence number of the message
     * @param json The request to send to the Pub/Sub server
     * @param isAwaitingServerResponse if true, the retrurned future will resolve when the server sends a response.  If false, will return immedialty with the sequence number in the JSON, with the following structure {"seq":seq}.
     *
     * @return CompletableFuture<JSONObject> future that will contain server response to given request, or the sequence number if we are not waiting on the server.
     */
    protected ListenableFuture<JSONObject> sendRequest(
            long sequence, JSONObject json, boolean isAwaitingServerResponse,
            PubSubErrorResponseHandler errorResponseHandler
    ) {
        SettableFuture<JSONObject> result = SettableFuture.create();

        try {
            if (isAwaitingServerResponse) {
                // If we're awaiting a server response, store this sequence number.
                outstanding.put(sequence, result);
            }

            if (errorResponseHandler != null) {
                publishErrorHandlers.put(sequence, errorResponseHandler);
                publishErrorHandlerRequests.put(sequence, json);
            }

            String jsonString = json.toString();
            Log.d("Cogs-SDK", "Sending JSON: " + jsonString);
            webSocketSession.send(json.toString());

            if (!isAwaitingServerResponse) {
                // If we're not awaiting a server response, resolve this promise immediately with the sequence number.
                JSONObject seqResponse = new JSONObject();
                seqResponse.put("seq", sequence);
                result.set(seqResponse);
            }
        } catch (Throwable t) {
            // Reject the promise if there is any exception.
            result.setException(t);
        }

        return result;
    }

    /**
     * Closes the underlying connection without preventing reconnects (Used for test purposes).
     *
     * @param dropOptions Options to fine-tune the effect of dropping the connection
     */
    protected void dropConnection(PubSubDropConnectionOptions dropOptions)
    {
        if(dropOptions != null) {
            if (dropOptions.getReconnectDelay().millis() < this.autoReconnectDelayMillis.get()) {
                autoReconnectDelayMillis.set(dropOptions.getReconnectDelay().millis());
            }
        }

        webSocketSession.close();
    }

    /**
     * Initiates the connection the the Pub/Sub server with the configuration for this PubSubSocket.
     * @throws Auth.AuthKeyError if the auth was an invalid format.
     */
    private ListenableFuture<PubSubSocket> connect() throws Auth.AuthKeyError {

        // Only allow one connect at a time.  Do nothing if we are already connecting.
        if (isSetupInProgress.compareAndSet(false, true) || setupFuture == null) {
            setupFuture = SettableFuture.create();
            Headers headers = new Headers();

            if (sessionUuid != null) {
                Log.d("Cogs-SDK", "connect() attempting to re-establish session with uuid ["+sessionUuid+"]");
            }

            Auth.PayloadHeaders ph = Auth.socketAuth(projectKeys, sessionUuid);
            headers.add("Host", options.getUri().getHost());
            headers.add("Payload", ph.payloadBase64);
            headers.add("PayloadHMAC", ph.payloadHmac);

            AsyncHttpRequest httpRequest = new AsyncHttpRequest(options.getUri(), "GET", headers);

            // This calls onCompleted(Exception error, WebSocket webSocket) on when complete:
            AsyncHttpClient.getDefaultInstance().websocket(httpRequest, "websocket", this);
        }

        return setupFuture;
    }

    /**
     * Attempts to reconnects a socket that has been dropped for any reason other than intentionally and cleanly disconnecting.
     * @return ListenableFuture<PubSubSocket> future that completes successfully when connected, with an error otherwise
     * @throws Auth.AuthKeyError Contains the auth cause of being unable to reconnect, if such occurs
     */
    private ListenableFuture<PubSubSocket> reconnect() throws Auth.AuthKeyError {
        // Connect, then call the reconnectHandler, then, if we are doing auto-reconnect, get the session UUID.
        ListenableFuture<PubSubSocket> connectFuture = connect();

        Function<PubSubSocket, PubSubSocket> reconnectHandlerFunction =
                new Function<PubSubSocket, PubSubSocket>() {
                    public PubSubSocket apply(PubSubSocket connectResponse) {
                        Log.d("Cogs-SDK", "reconnect() connection established.");

                        if (reconnectHandler != null) {
                            reconnectHandler.onReconnect();
                        }

                        return connectResponse;
                    }
                };

        ListenableFuture<PubSubSocket> reconnectHandlerFuture = Futures.transform(connectFuture, reconnectHandlerFunction, executor);

        return reconnectHandlerFuture;
    }

    private synchronized void startKeepAliveHearbeatPing() {
        stopKeepAliveHeartbeatPing();
        keepAliveFuture = keepAliveExecutorSerivce.scheduleAtFixedRate(keepAliveRunnable, keepAliveIntervalMs, keepAliveIntervalMs, TimeUnit.MILLISECONDS);
    }

    private synchronized void stopKeepAliveHeartbeatPing() {
        if (keepAliveFuture!=null) {
            keepAliveFuture.cancel(true);
        }
    }

    private void connectSucceeded() {
        if (setupFuture != null) {
            setupFuture.set(PubSubSocket.this);
        }

        isConnected.set(true);
        startKeepAliveHearbeatPing();
    }

    private void connectFailed(Throwable cause) {
        if (setupFuture != null) {
            setupFuture.setException(cause);
        }

        if (errorHandler != null) {
            errorHandler.onError(new RuntimeException("Error establishing connection to Pub/Sub server.", cause));
        }
    }

    ///////////////////// EXTENDING ENDPOINT AND IMPLEMENTING MESSAGE_HANDLER ///////////////////// 

    /**
     * Called immediately after establishing the connection represented by this PubSubSocket
     * @param error The configuration used to establish this PubSubSocket
     * @param webSocket The session that has just been activated by this PubSubSocket
     */
    @Override
    public void onCompleted(Exception error, WebSocket webSocket) {
        isSetupInProgress.set(false);
        if (error != null) {
            Log.e("Cogs-SDK", "Error on subscription WebSocket connect.", error);
            connectFailed(error);
        } else if (webSocket == null) {
            Log.e("Cogs-SDK", "Error on subscription WebSocket connect - could not connect.");
            connectFailed(new Exception("Error on subscription WebSocket connect - could not connect - null websocket."));
        } else {
            Log.d("Cogs-SDK", "Successfully connected.");
            PubSubSocket.this.webSocketSession = webSocket;

            webSocket.setStringCallback(PubSubSocket.this);
            webSocket.setClosedCallback(PubSubSocket.this);
            webSocket.setEndCallback(new CompletedCallback() {
                @Override
                public void onCompleted(Exception ex) {
                    Log.d("Cogs-SDK", "setEndCallback got exception:", ex);
                }
            });

            if (!autoReconnect.get()) {
                connectSucceeded();
            } else {
                // If we are auto-reconnecting, get the uuid, and notify the newSessionHandler if it has changed.
                ListenableFuture<UUID> getSessionUuidFuture = new PubSubHandle(PubSubSocket.this, -1L).getSessionUuid();

                Futures.addCallback(getSessionUuidFuture, new FutureCallback<UUID>() {
                    public void onSuccess(UUID getSessionUuidResponse) {

                        Log.d("Cogs-SDK", "Comparing client uuid["+PubSubSocket.this.sessionUuid+"] with server uuid["+getSessionUuidResponse+"]");

                        // If there was a uuid change, notify the listener.
                        if (!getSessionUuidResponse.equals(PubSubSocket.this.sessionUuid)) {
                            PubSubSocket.this.sessionUuid = getSessionUuidResponse;
                            if (PubSubSocket.this.newSessionHandler != null) {
                                PubSubSocket.this.newSessionHandler.onNewSession(getSessionUuidResponse);
                            }
                        }

                        connectSucceeded();
                    }
                    public void onFailure(Throwable error) {
                        connectFailed(error);

                        // This will trigger another reconnect attempt.
                        isConnected.set(false);
                        stopKeepAliveHeartbeatPing();
                    }
                }, executor);
            }
        }
    }

    // TODO: complete, fix, or remove the following
    /*
    @Override
    public void onOpen(Session session, EndpointConfig config) {
        isConnected.set(true);
        session.setMessageHandler(this);
    }*/

    /**
     * Called immediately before closing the connection represented by this PubSubSocket
     * @param closeReason The reason for closing this PubSubSocket
     */
    @Override
    public void onCompleted(Exception closeReason) {
        long previousDelay;
        long minimumDelay;
        long nextDelay;

        isConnected.set(false);
        stopKeepAliveHeartbeatPing();

        if(closeHandler != null) {
            closeHandler.onClose(closeReason);
        }

        if (closeReason != null) {
            Log.e("Cogs-SDK", "Error caused WebSocket to close.", closeReason);
        } else {
            Log.d("Cogs-SDK", "WebSocket closed without error.");
        }

        if(options.getAutoReconnect() == true) {
            Log.d("Cogs-SDK", "Lost connection.  Attempting reconnect.", closeReason);
            reconnectRetry(0);
        }
    }

    /**
     * Wait the specified time, attempt to reconnect, then, if there is a failure, call reconnectRetry again.
     *
     * @param msUntilNextRetry
     */
    private void reconnectRetry(final long msUntilNextRetry) {
        Log.d("Cogs-SDK", "Attempting reconnect in "+msUntilNextRetry+"ms.");
        keepAliveExecutorSerivce.schedule(new Runnable()
        {
            public void run() {
                try {
                    ListenableFuture<PubSubSocket> connectFuture = reconnect();

                    Futures.addCallback(connectFuture, new FutureCallback<PubSubSocket>() {
                        public void onSuccess(PubSubSocket psh) {
                            Log.d("Cogs-SDK", "Successfully reconnected after lost connection.");
                        }

                        public void onFailure(Throwable error) {

                            long minimumDelay = Math.max(PubSubSocket.this.autoReconnectDelayMillis.get(), msUntilNextRetry);
                            long nextDelay = Math.min(minimumDelay * 2, MAX_RECONNECT_DELAY_MILLIS);

                            // Attempt reconnecting forever.
                            reconnectRetry(nextDelay);
                        }
                    }, executor);
                } catch (Auth.AuthKeyError ake) {
                    Log.e("Cogs-SDK", "Auth error when reconnecting", ake);
                }
            }
        }, msUntilNextRetry, TimeUnit.MILLISECONDS);
    }

    /**
     * Called whenever the connection represented by this PubSubSocket produces errors
     *
     * @param session The session that has produced an error
     * @param throwable The error that was thrown involving the session
     *//*
    @Override
    public void onError(Session session, Throwable throwable) {
        if (errorHandler != null) {
            errorHandler.onError(throwable);
        }
    }*/

    /**
     * Called when receiving messages from the remote endpoint (Pub/Sub server). 
     * The method proprogates Pub/Sub messages to appropriate channels when it receives them,
     * and completes outstanding futures when receiving response to other requests.
     * @param message The message received from the remote endpoint
     */
    @Override
    public void onStringAvailable(String message) {
        Log.d("Cogs-SDK","Received JSON: "+message);

        if(rawRecordHandler != null) {
            rawRecordHandler.onRawRecord(message);
        }

        // TODO: [PUB-316] validate format of message received from server, if invalid call error

        try {
            JsonNode json = Json.parseOrThrow(message);

            if ("msg".equals(json.str("action"))) {
                String id = json.str("id");
                String msg = json.str("msg");
                String time = json.str("time");
                String chan = json.str("chan");

                PubSubMessageRecord record = new PubSubMessageRecord(chan, msg, time, id);
                PubSubMessageHandler handler = msgHandlers.get(chan);

                handler.onMessage(record);

                if (generalMsgHandler != null) {
                    generalMsgHandler.onMessage(record);
                }
            } else if (!json.has("seq")) {
                // This should never happen.  Log it and send to the handler, if specified.
                PubSubException error = new PubSubException("Expected sequence number missing: "+message);
                Log.e("Cogs-SDK","Expected sequence number missing.", error);

                if (errorHandler != null) {
                    errorHandler.onError(error);
                }
            } else {
                long sequence = json.num("seq").longValue();
                String action = json.str("action");
                int code = json.num("code").intValue();
                String channel = json.str("channel");

                // Resolve a promise if there is a future waiting for a response.
                SettableFuture<JSONObject> responseFuture = outstanding.getIfPresent(sequence);

                if (responseFuture != null) {
                    if (code == 200) {
                        responseFuture.set(json.asObject());
                    } else {
                        responseFuture.setException(new PubSubException("Received an error from the server:" + message));
                    }

                    outstanding.invalidate(sequence);
                } else {
                    // All responses with a seq should have a future waiting for it.
                    Log.w("Cogs-SDK","Recieved a message with a sequence number, but no client handler is listening for it: "+message);
                }

                if ("pub".equals(action)) {
                    // Notify the error handler if the server Resolve a promise if there is a future waiting for a response.
                    PubSubErrorResponseHandler errorHandler = publishErrorHandlers.getIfPresent(sequence);

                    if (errorHandler != null) {
                        JSONObject request = publishErrorHandlerRequests.getIfPresent(sequence);

                        if (request != null) {
                            channel = new JsonObject(request).str("chan");
                        }

                        errorHandler.onError(sequence, action, code, channel);
                    }
                }

                if (code != 200 && errorResponseHandler != null) {
                    errorResponseHandler.onError(sequence, action, code, channel);
                }
            }
        } catch (JSONException e) {
            // This should never happen.  Log it and send to the handler, if specified.
            PubSubException error = new PubSubException("Could not parse response from server: "+message, e);
            Log.e("Cogs-SDK","Could not parse response from server.", error);

            if (errorHandler != null) {
                errorHandler.onError(error);
            }
        }
    }

    /**
     * Set the session ID.  This protected method is to be used for testing only.
     * @param sessionUuid
     */
    protected void setSessionUuid(UUID sessionUuid) {
        this.sessionUuid = sessionUuid;
    }

    /**
     * Get the session ID.  This protected method is to be used for testing only.
     * @return sessionUuid
     */
    protected UUID getSessionUuid() {
        return sessionUuid;
    }

    /**
     * Handler called when keep-alive pings are sent.  This is protected for testing.
     * @param handler The handler to call
     */
    protected void setKeepAliveHandler(Runnable handler) {
        keepAliveHandler = handler;
    }

    /**
     * Registers a handler to call whenever a new session is generated by the server
     * @param handler The handler to call
     */
    public void setNewSessionHandler(PubSubNewSessionHandler handler) {
        newSessionHandler = handler;
    }

    /**
     * Registers a handler that will be called any time the underlying socket must be reconnected
     * @param handler The handler to register for the reconnects
     */
    public void setReconnectHandler(PubSubReconnectHandler handler) {
        reconnectHandler = handler;
    }

    /**
     * Register a handler to call whenever a raw record (string json) is received from the server.
     * @param handler The handler to register
     */
    public void setRawRecordHandler(PubSubRawRecordHandler handler) {
        rawRecordHandler = handler;
    }

    /**
     * Registers a handler to call if there are failures working with the underlying socket
     * @param handler The handler to register
     */
    public void setErrorHandler(PubSubErrorHandler handler) {
        errorHandler = handler;
    }

    /**
     * Registers a handler to call for any all responses from the server.
     *
     * @param handler The {@link PubSubErrorResponseHandler handler} to register
     */
    public void setErrorResponseHandler(PubSubErrorResponseHandler handler) {
        errorResponseHandler = handler;
    }

    /**
     * Register a handler to call whenever the underlying socket is actually closed.
     * @param handler The handler to register
     */
    public void setCloseHandler(PubSubCloseHandler handler) {
        closeHandler = handler;
    }

    /**
     * Registers a general handler that receives and handles message from all channels.
     * @param handler The handler to be registered
     */
    public void setMessageHandler(PubSubMessageHandler handler) {
        generalMsgHandler = handler;
    }

    /**
     * Associates a {@link PubSubMessageHandler} to call for message received from the given channel.
     * @param channel The channel with which to associate the given handler
     * @param handler The {@link PubSubMessageHandler} that will be called for message from the given channel.
     */
    public void setMessageHandler(String channel, PubSubMessageHandler handler) {
        msgHandlers.put(channel, handler);
    }

    /**
     * Disassociates the current {@link PubSubMessageHandler}, if any, with the given channel.
     * @param channel The channel from which to remove the handler
     */
    public void removeMessageHandler(String channel) {
        msgHandlers.remove(channel);
    }

    /**
     * Dissacociates all {@link PubSubMessageHandler message handlers}, if any, from this socket.
     */
    public void removeAllMessageHandlers() {
        msgHandlers.clear();
    }
}