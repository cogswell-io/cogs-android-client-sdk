package io.cogswell.sdk.pubsub;

import android.util.Log;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import io.cogswell.sdk.pubsub.handlers.PubSubCloseHandler;
import io.cogswell.sdk.pubsub.handlers.PubSubErrorHandler;
import io.cogswell.sdk.pubsub.handlers.PubSubNewSessionHandler;
import io.cogswell.sdk.pubsub.handlers.PubSubReconnectHandler;

public class PubSubSocketTest extends TestCase {

    private Executor executor = MoreExecutors.directExecutor();
    private static int asyncTimeoutSeconds = 30;

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
        final Map<String,Object> responses = new HashMap<String, Object>();
        final CountDownLatch signal = new CountDownLatch(1);

        ListenableFuture<PubSubSocket> connectFuture = PubSubSocket.connectSocket(keys, new PubSubOptions(host));

        assertNotNull(connectFuture);
        Futures.addCallback(connectFuture, new FutureCallback<PubSubSocket>() {
            public void onSuccess(PubSubSocket pubsubSocket) {
                responses.put("pubsubSocket", pubsubSocket);
                signal.countDown();
            }
            public void onFailure(Throwable error) {
                Log.e("TEST","Error:",error);
                responses.put("pubsubSocket", error);
                signal.countDown();
            }
        }, executor);

        signal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertTrue(responses.get("pubsubSocket") instanceof PubSubSocket);
    }


    public void testGetSessionSuccessful() throws Exception {
        final Map<String,Object> responses = new HashMap<String, Object>();
        final CountDownLatch signal = new CountDownLatch(1);
        final long seqNum = 123;
        final JSONObject getSessionRequest = new JSONObject()
                .put("seq", seqNum)
                .put("action", "session-uuid");

        ListenableFuture<PubSubSocket> connectFuture = PubSubSocket.connectSocket(keys, new PubSubOptions(host));
        AsyncFunction<PubSubSocket, JSONObject> getSessionFunction =
                new AsyncFunction<PubSubSocket, JSONObject>() {
                    public ListenableFuture<JSONObject> apply(PubSubSocket pubsubSocket) {
                        responses.put("pubsubSocket", pubsubSocket);
                        return pubsubSocket.sendRequest(seqNum, getSessionRequest, true, null);
                    }
                };
        ListenableFuture<JSONObject> getSessionFuture = Futures.transformAsync(connectFuture, getSessionFunction, executor);

        Futures.addCallback(getSessionFuture, new FutureCallback<JSONObject>() {
            public void onSuccess(JSONObject getSessionResponse) {
                responses.put("getSessionResponse", getSessionResponse);
                signal.countDown();
            }
            public void onFailure(Throwable error) {
                Log.e("TEST","Error:",error);
                responses.put("getSessionResponse", error);
                signal.countDown();
            }
        }, executor);

        signal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertTrue(responses.get("getSessionResponse") instanceof JSONObject);
        assertTrue(responses.get("getSessionResponse").toString().contains("uuid"));
    }


    public void testReconnect() throws Exception {
        final Map<String,Object> responses = new HashMap<>();
        final CountDownLatch signalClosed = new CountDownLatch(1);
        final CountDownLatch signalReconnected = new CountDownLatch(1);
        final long seqNum = 123;
        final Exception expectedConnectionException = new Exception();
        final JSONObject getSessionRequest = new JSONObject()
                .put("seq", seqNum)
                .put("action", "session-uuid");
        final PubSubCloseHandler closeHandler = new PubSubCloseHandler() {
            @Override
            public void onClose(Throwable error) {
                responses.put("onCloseError", error);
                signalClosed.countDown();
            }
        };
        final PubSubReconnectHandler reconnectHandler = new PubSubReconnectHandler() {
            @Override
            public void onReconnect() {
                responses.put("onReconnect", true);
                signalReconnected.countDown();
            }
        };
        final PubSubNewSessionHandler pubSubNewSessionHandler = new PubSubNewSessionHandler() {
            @Override
            public void onNewSession(UUID uuid) {
                responses.put("onNewSession", uuid);
            }
        };

        PubSubOptions pubSubOptions = new PubSubOptions(host, true, 30000L, null);
        ListenableFuture<PubSubSocket> connectFuture = PubSubSocket.connectSocket(keys, pubSubOptions);

        Futures.addCallback(connectFuture, new FutureCallback<PubSubSocket>() {
            public void onSuccess(PubSubSocket pubsubSocket) {
                responses.put("pubsubSocket", pubsubSocket);
                pubsubSocket.setCloseHandler(closeHandler);
                pubsubSocket.setReconnectHandler(reconnectHandler);
                pubsubSocket.setNewSessionHandler(pubSubNewSessionHandler);

                // Force an unplanned closing.
                pubsubSocket.onCompleted(expectedConnectionException);
            }
            public void onFailure(Throwable error) {
                Log.e("TEST","Error:",error);
                responses.put("pubsubSocket", error);
            }
        }, executor);

        // Wait until the closeHandler function is called.
        signalClosed.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertTrue(responses.get("onCloseError") instanceof Throwable);
        assertEquals(expectedConnectionException, responses.get("onCloseError"));

        // Wait until the reconnectHandler function is called.
        signalReconnected.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertEquals(true, responses.get("onReconnect"));
        assertTrue(responses.get("onNewSession") == null);
    }


    public void testReconnectWithSessionUuidChange() throws Exception {
        final Map<String,Object> responses = new HashMap<>();
        final CountDownLatch signalClosed = new CountDownLatch(1);
        final CountDownLatch signalReconnected = new CountDownLatch(1);
        final long seqNum = 123;
        final Exception expectedConnectionException = new Exception();
        final UUID forcedChangeToUuidToTriggerNotification = UUID.randomUUID();
        final JSONObject getSessionRequest = new JSONObject()
                .put("seq", seqNum)
                .put("action", "session-uuid");
        final PubSubCloseHandler closeHandler = new PubSubCloseHandler() {
            @Override
            public void onClose(Throwable error) {
                responses.put("onCloseError", error);
                signalClosed.countDown();
            }
        };
        final PubSubReconnectHandler reconnectHandler = new PubSubReconnectHandler() {
            @Override
            public void onReconnect() {
                responses.put("onReconnect", true);
                signalReconnected.countDown();
            }
        };
        final PubSubNewSessionHandler pubSubNewSessionHandler = new PubSubNewSessionHandler() {
            @Override
            public void onNewSession(UUID uuid) {
                responses.put("onNewSession", uuid);
            }
        };

        PubSubOptions pubSubOptions = new PubSubOptions(host, true, 30000L, null);
        ListenableFuture<PubSubSocket> connectFuture = PubSubSocket.connectSocket(keys, pubSubOptions);

        Futures.addCallback(connectFuture, new FutureCallback<PubSubSocket>() {
            public void onSuccess(PubSubSocket pubsubSocket) {
                responses.put("pubsubSocket", pubsubSocket);
                responses.put("pubsubSocket.sessionUuid", pubsubSocket.getSessionUuid());
                pubsubSocket.setCloseHandler(closeHandler);
                pubsubSocket.setReconnectHandler(reconnectHandler);
                pubsubSocket.setNewSessionHandler(pubSubNewSessionHandler);

                // Force an unplanned closing.
                pubsubSocket.onCompleted(expectedConnectionException);
                // Force a session UUID change notification
                pubsubSocket.setSessionUuid(forcedChangeToUuidToTriggerNotification);
            }
            public void onFailure(Throwable error) {
                Log.e("TEST","Error:",error);
                responses.put("pubsubSocket", error);
            }
        }, executor);

        // Wait until the closeHandler function is called.
        signalClosed.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertTrue(responses.get("onCloseError") instanceof Throwable);
        assertEquals(expectedConnectionException, responses.get("onCloseError"));

        // Wait until the reconnectHandler function is called.
        signalReconnected.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertEquals(true, responses.get("onReconnect"));

        // The client uuid before the reset (pubsubSocket.sessionUuid) should not match the client uuid after the reset (onNewSession)
        assertTrue(!responses.get("pubsubSocket.sessionUuid").equals(responses.get("onNewSession")));
    }


    public void testKeepAlive() throws Exception {
        final Map<String,Object> responses = new HashMap<String, Object>();

        final CountDownLatch signal = new CountDownLatch(1);
        long pingIntervalSeconds = 30*2;

        ListenableFuture<PubSubSocket> connectFuture = PubSubSocket.connectSocket(keys, new PubSubOptions(host));

        assertNotNull(connectFuture);
        Futures.addCallback(connectFuture, new FutureCallback<PubSubSocket>() {
            public void onSuccess(PubSubSocket pubsubSocket) {
                pubsubSocket.setKeepAliveHandler(new Runnable() {
                    @Override
                    public void run() {

                        // Only trigger the count down latch after we've pinged the server.
                        responses.put("keepAliveHandler", true);
                        signal.countDown();
                    }
                });
            }
            public void onFailure(Throwable error) {
                Log.e("TEST","Error:",error);
            }
        }, executor);

        signal.await(asyncTimeoutSeconds + pingIntervalSeconds, TimeUnit.SECONDS);

        assertEquals(true, responses.get("keepAliveHandler"));
    }


    public void testPublishWithServerError() throws Exception {
        final Map<String,Object> responses = new HashMap<String, Object>();
        final CountDownLatch signal = new CountDownLatch(1);
        final String testChannel = "TEST-CHANNEL";
        final Long testSeqNumber = 123L;

        PubSubSocket pss = new PubSubSocket(new WebSocket() {
            @Override
            public void send(byte[] bytes) {}

            @Override
            public void send(String string) {}

            @Override
            public void send(byte[] bytes, int offset, int len) {}

            @Override
            public void ping(String message) {}

            @Override
            public void setStringCallback(StringCallback callback) {}

            @Override
            public StringCallback getStringCallback() { return null; }

            @Override
            public void setPingCallback(PingCallback callback) {}

            @Override
            public void setPongCallback(PongCallback callback) {}

            @Override
            public PongCallback getPongCallback() { return null; }

            @Override
            public boolean isBuffering() { return false; }

            @Override
            public AsyncSocket getSocket() { return null; }

            @Override
            public AsyncServer getServer() { return null; }

            @Override
            public void setDataCallback(DataCallback callback) {}

            @Override
            public DataCallback getDataCallback() { return null; }

            @Override
            public boolean isChunked() { return false;}

            @Override
            public void pause() {}

            @Override
            public void resume() {}

            @Override
            public void close() {}

            @Override
            public boolean isPaused() { return false;}

            @Override
            public void setEndCallback(CompletedCallback callback) {}

            @Override
            public CompletedCallback getEndCallback() { return null; }

            @Override
            public String charset() { return null; }

            @Override
            public void write(ByteBufferList bb) {}

            @Override
            public void setWriteableCallback(WritableCallback handler) {}

            @Override
            public WritableCallback getWriteableCallback() { return null; }

            @Override
            public boolean isOpen() { return false; }

            @Override
            public void end() {}

            @Override
            public void setClosedCallback(CompletedCallback handler) {}

            @Override
            public CompletedCallback getClosedCallback() { return null; }
        });
        JSONObject requestJson = new JSONObject()
                .put("seq", testSeqNumber)
                .put("action", "pub")
                .put("chan", testChannel)
                .put("msg", "test-message")
                .put("ack", false);
        pss.sendRequest(testSeqNumber, requestJson, false, new PubSubErrorHandler() {
            @Override
            public void onError(Throwable error, Long sequence, String channel) {
                responses.put("onError.sequence", sequence);
                responses.put("onError.channel", channel);
                signal.countDown();

            }
        });
        JSONObject responseJson = new JSONObject()
                .put("seq", testSeqNumber)
                .put("action", "pub")
                .put("code", 401)
                .put("message", "Not Authorized Test")
                .put("details", "Not Authorized Test Details");
        pss.onStringAvailable(responseJson.toString());

        signal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertEquals(testSeqNumber, responses.get("onError.sequence"));
        assertEquals(testChannel, responses.get("onError.channel"));
    }

}