package io.cogswell.sdk.pubsub;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.koushikdutta.async.AsyncServer;
import com.koushikdutta.async.AsyncSocket;
import com.koushikdutta.async.ByteBufferList;
import com.koushikdutta.async.callback.CompletedCallback;
import com.koushikdutta.async.callback.DataCallback;
import com.koushikdutta.async.callback.WritableCallback;
import com.koushikdutta.async.http.WebSocket;

import junit.framework.TestCase;

import org.json.JSONObject;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.cogswell.sdk.json.JsonNode;
import io.cogswell.sdk.json.JsonNull;
import io.cogswell.sdk.json.JsonObject;
import io.cogswell.sdk.pubsub.handlers.PubSubCloseHandler;
import io.cogswell.sdk.pubsub.handlers.PubSubErrorResponseHandler;
import io.cogswell.sdk.pubsub.handlers.PubSubNewSessionHandler;
import io.cogswell.sdk.pubsub.handlers.PubSubReconnectHandler;
import io.cogswell.sdk.utils.Container;
import io.cogswell.sdk.utils.Duration;

public class PubSubSocketTest extends TestCase {
    private Executor executor = Executors.newFixedThreadPool(16);
    private static int asyncTimeoutSeconds = 4;

    private List<String> keys = new ArrayList<String>();
    private String host = null;

    @Override
    protected void setUp() throws Exception {
        InputStream jsonConfigIS = this.getClass().getResourceAsStream("config.json");
        String configJsonString = new Scanner(jsonConfigIS, "UTF-8").useDelimiter("\\A").next();

        JSONObject configJson = new JSONObject(configJsonString);

        // Get the host.
        host = configJson.optString("host", null);

        // Add the keys
        JSONObject keysJson = configJson.getJSONObject("keys");
        String rKey = keysJson.optString("readKey", null);

        if (rKey != null) {
            keys.add(rKey);
        }

        String wKey = keysJson.optString("writeKey", null);

        if (wKey != null) {
            keys.add(wKey);
        }

        String aKey = keysJson.optString("adminKey", null);

        if (aKey != null) {
            keys.add(aKey);
        }
    }

    public void testConnect() throws Exception {
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>();

        ListenableFuture<PubSubSocket> connectFuture = PubSubSocket.connectSocket(keys, new PubSubOptions(host));

        assertNotNull(connectFuture);
        Futures.addCallback(connectFuture, new FutureCallback<PubSubSocket>() {
            public void onSuccess(PubSubSocket pubsubSocket) {
                queue.offer("success");
            }
            public void onFailure(Throwable error) {
                queue.offer("failure");
            }
        }, executor);

        assertEquals("success", queue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
    }

    public void testGetSessionSuccessful() throws Exception {
        final BlockingQueue<JsonNode> queue = new LinkedBlockingQueue<>();
        final Container<PubSubSocket> socket = new Container<>();

        final long seqNum = 123;
        final JSONObject getSessionRequest = new JSONObject()
                .put("seq", seqNum)
                .put("action", "session-uuid");

        ListenableFuture<PubSubSocket> connectFuture = PubSubSocket.connectSocket(keys, new PubSubOptions(host));
        AsyncFunction<PubSubSocket, JSONObject> getSessionFunction =
                new AsyncFunction<PubSubSocket, JSONObject>() {
                    public ListenableFuture<JSONObject> apply(PubSubSocket pubsubSocket) {
                        return socket.set(pubsubSocket).sendRequest(seqNum, getSessionRequest, true, null);
                    }
                };
        ListenableFuture<JSONObject> getSessionFuture = Futures.transformAsync(connectFuture, getSessionFunction, executor);

        Futures.addCallback(getSessionFuture, new FutureCallback<JSONObject>() {
            public void onSuccess(JSONObject getSessionResponse) {
                queue.offer(new JsonObject(getSessionResponse));
            }
            public void onFailure(Throwable error) {
                queue.offer(JsonNull.singleton);
            }
        }, executor);

        try {
            JsonNode response = queue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS);

            assertNotNull(response);
            assertFalse(response.isNull());
            assertNotNull(response.str("uuid"));
            assertEquals(seqNum, response.num("seq").longValue());
            assertEquals("session-uuid", response.str("action"));
            assertEquals(200, response.num("code").intValue());
        } finally {
            socket.get().close();
        }
    }

    public void testReconnect() throws Exception {
        final Container<UUID> newSession = new Container<>();
        final BlockingQueue<String> reconnectQueue = new LinkedBlockingQueue<>(1);
        final BlockingQueue<String> closeQueue = new LinkedBlockingQueue<>(1);

        final long seqNum = 123;
        final Exception expectedConnectionException = new Exception();
        final JSONObject getSessionRequest = new JSONObject()
                .put("seq", seqNum)
                .put("action", "session-uuid");

        final PubSubCloseHandler closeHandler = new PubSubCloseHandler() {
            public void onClose(Throwable error) {
                if (error == null) {
                    closeQueue.offer("closed-without-cause");
                } else {
                    closeQueue.offer("closed");
                }
            }
        };

        final PubSubReconnectHandler reconnectHandler = new PubSubReconnectHandler() {
            public void onReconnect() {
                reconnectQueue.offer("reconnected");
            }
        };

        final PubSubNewSessionHandler pubSubNewSessionHandler = new PubSubNewSessionHandler() {
            public void onNewSession(UUID uuid) {
                newSession.set(uuid);
            }
        };

        PubSubOptions pubSubOptions = new PubSubOptions(host, true, Duration.of(30, TimeUnit.SECONDS), null);
        ListenableFuture<PubSubSocket> connectFuture = PubSubSocket.connectSocket(keys, pubSubOptions);

        Futures.addCallback(connectFuture, new FutureCallback<PubSubSocket>() {
            public void onSuccess(PubSubSocket pubsubSocket) {
                pubsubSocket.setCloseHandler(closeHandler);
                pubsubSocket.setReconnectHandler(reconnectHandler);
                pubsubSocket.setNewSessionHandler(pubSubNewSessionHandler);

                // Force an unplanned closing.
                pubsubSocket.onCompleted(expectedConnectionException);
            }
            public void onFailure(Throwable error) {
                closeQueue.offer("connect-failure");
            }
        }, executor);

        assertEquals("closed", closeQueue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
        assertEquals("reconnected", reconnectQueue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
        assertNull(newSession.get());
    }

    public void testReconnectWithSessionUuidChange() throws Exception {
        final Container<PubSubSocket> socket = new Container<>();
        final Container<UUID> oldSessionUuid = new Container<>();
        final Container<UUID> newSessionUuid = new Container<>();

        final BlockingQueue<String> reconnectQueue = new LinkedBlockingQueue<>(1);
        final BlockingQueue<String> closeQueue = new LinkedBlockingQueue<>(1);

        final long seqNum = 123;
        final Exception expectedConnectionException = new Exception();
        final UUID forcedChangeToUuidToTriggerNotification = UUID.randomUUID();

        final JSONObject getSessionRequest = new JSONObject()
                .put("seq", seqNum)
                .put("action", "session-uuid");

        final PubSubCloseHandler closeHandler = new PubSubCloseHandler() {
            public void onClose(Throwable error) {
                if (error == null) {
                    closeQueue.offer("closed-without-cause");
                } else {
                    closeQueue.offer("closed");
                }
            }
        };

        final PubSubReconnectHandler reconnectHandler = new PubSubReconnectHandler() {
            public void onReconnect() {
                reconnectQueue.offer("reconnected");
            }
        };

        final PubSubNewSessionHandler pubSubNewSessionHandler = new PubSubNewSessionHandler() {
            public void onNewSession(UUID uuid) {
                newSessionUuid.set(uuid);
            }
        };

        PubSubOptions pubSubOptions = new PubSubOptions(host, true, Duration.of(30, TimeUnit.SECONDS), null);
        ListenableFuture<PubSubSocket> connectFuture = PubSubSocket.connectSocket(keys, pubSubOptions);

        Futures.addCallback(connectFuture, new FutureCallback<PubSubSocket>() {
            public void onSuccess(PubSubSocket pubsubSocket) {
                socket.set(pubsubSocket);
                oldSessionUuid.set(pubsubSocket.getSessionUuid());

                pubsubSocket.setCloseHandler(closeHandler);
                pubsubSocket.setReconnectHandler(reconnectHandler);
                pubsubSocket.setNewSessionHandler(pubSubNewSessionHandler);

                // Force a session UUID change notification
                pubsubSocket.setSessionUuid(forcedChangeToUuidToTriggerNotification);

                // Force an unplanned closing.
                pubsubSocket.onCompleted(expectedConnectionException);
            }
            public void onFailure(Throwable error) {
                closeQueue.offer("connect-failure");
            }
        }, executor);

        assertEquals("reconnected", reconnectQueue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
        assertEquals("closed", closeQueue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));

        assertNotSame(oldSessionUuid.get(), newSessionUuid.get());
    }

    public void testKeepAlive() throws Exception {
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>(1);

        long pingIntervalSeconds = 30*2;

        ListenableFuture<PubSubSocket> connectFuture = PubSubSocket.connectSocket(keys, new PubSubOptions(host));

        assertNotNull(connectFuture);
        Futures.addCallback(connectFuture, new FutureCallback<PubSubSocket>() {
            public void onSuccess(PubSubSocket pubsubSocket) {
                pubsubSocket.setKeepAliveHandler(new Runnable() {
                    public void run() {
                        // Only signal completion once we have pinged the server
                        queue.offer("success");
                    }
                });
            }
            public void onFailure(Throwable error) {
                queue.offer("connect-failure");
            }
        }, executor);

        assertEquals("success", queue.poll(asyncTimeoutSeconds + pingIntervalSeconds, TimeUnit.SECONDS));
    }

    public void testPublishWithServerError() throws Exception {
        final BlockingQueue<JsonNode> queue = new LinkedBlockingQueue<>(1);

        final Map<String,Object> responses = new HashMap<String, Object>();
        final CountDownLatch signal = new CountDownLatch(1);

        final String testChannel = "TEST-CHANNEL";
        final Long testSeqNumber = 123L;

        PubSubSocket pss = new PubSubSocket(new WebSocket() {
            @Override public void send(byte[] bytes) {}
            @Override public void send(String string) {}
            @Override public void send(byte[] bytes, int offset, int len) {}
            @Override public void ping(String message) {}
            @Override public void setStringCallback(StringCallback callback) {}
            @Override public StringCallback getStringCallback() { return null; }
            @Override public void setPingCallback(PingCallback callback) {}
            @Override public void setPongCallback(PongCallback callback) {}
            @Override public PongCallback getPongCallback() { return null; }
            @Override public boolean isBuffering() { return false; }
            @Override public AsyncSocket getSocket() { return null; }
            @Override public AsyncServer getServer() { return null; }
            @Override public void setDataCallback(DataCallback callback) {}
            @Override public DataCallback getDataCallback() { return null; }
            @Override public boolean isChunked() { return false;}
            @Override public void pause() {}
            @Override public void resume() {}
            @Override public void close() {}
            @Override public boolean isPaused() { return false;}
            @Override public void setEndCallback(CompletedCallback callback) {}
            @Override public CompletedCallback getEndCallback() { return null; }
            @Override public String charset() { return null; }
            @Override public void write(ByteBufferList bb) {}
            @Override public void setWriteableCallback(WritableCallback handler) {}
            @Override public WritableCallback getWriteableCallback() { return null; }
            @Override public boolean isOpen() { return false; }
            @Override public void end() {}
            @Override public void setClosedCallback(CompletedCallback handler) {}
            @Override public CompletedCallback getClosedCallback() { return null; }
        });

        JSONObject requestJson = new JSONObject()
                .put("seq", testSeqNumber)
                .put("action", "pub")
                .put("chan", testChannel)
                .put("msg", "test-message")
                .put("ack", false);

        pss.sendRequest(testSeqNumber, requestJson, false, new PubSubErrorResponseHandler() {
            @Override
            public void onErrorResponse(Long sequence, String action, Integer code, String channel) {
                JSONObject obj = new JSONObject();
                try {
                    if (sequence != null) obj.put("seq", sequence);
                    if (sequence != null) obj.put("action", action);
                    if (sequence != null) obj.put("code", code);
                    if (channel != null) obj.put("channel", channel);

                    queue.offer(new JsonObject(obj));
                } catch(Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        JSONObject responseJson = new JSONObject()
                .put("seq", testSeqNumber)
                .put("action", "pub")
                .put("code", 401)
                .put("message", "Not Authorized Test")
                .put("details", "Not Authorized Test Details");

        pss.onStringAvailable(responseJson.toString());

        JsonNode expectedResponse = new JsonObject(responseJson);
        JsonNode realResponse = queue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertNotNull(realResponse);

        assertEquals(realResponse.str("seq"), expectedResponse.str("seq"));
        assertEquals(realResponse.str("code"), expectedResponse.str("code"));
        assertEquals(realResponse.str("action"), expectedResponse.str("action"));
        assertEquals(realResponse.str("channel"), testChannel);
    }
}