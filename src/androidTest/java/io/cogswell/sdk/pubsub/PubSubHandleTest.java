package io.cogswell.sdk.pubsub;

import android.util.Log;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

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

import io.cogswell.sdk.pubsub.handlers.PubSubMessageHandler;

public class PubSubHandleTest extends TestCase {
    Object result = null;
    private static int asyncTimeoutSeconds = 10;
    private Executor executor = new Executor() {
        public void execute(Runnable r) {
            new Thread(r).start();
            //r.run();
        }
    };//MoreExecutors.directExecutor();

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
        result = null;

        final CountDownLatch signal = new CountDownLatch(1);

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        assertNotNull(connectFuture);
        Futures.addCallback(connectFuture, new FutureCallback<PubSubHandle>() {
            public void onSuccess(PubSubHandle psh) {
                PubSubHandleTest.this.result = psh;
                signal.countDown();
            }
            public void onFailure(Throwable error) {
                Log.e("TEST","Error:",error);
                PubSubHandleTest.this.result = error;
                signal.countDown();
            }
        });

        signal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertTrue(PubSubHandleTest.this.result instanceof PubSubHandle);
    }

    public void testGetSessionUuid() throws Exception {
        final Map<String,Object> responses = new HashMap<String, Object>();
        final CountDownLatch signal = new CountDownLatch(1);

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, UUID> getSessionUuidFunction =
                new AsyncFunction<PubSubHandle, UUID>() {
                    public ListenableFuture<UUID> apply(PubSubHandle pubsubHandle) {
                        return pubsubHandle.getSessionUuid();
                    }
                };
        ListenableFuture<UUID> getSessionUuidFuture = Futures.transformAsync(connectFuture, getSessionUuidFunction, executor);

        Futures.addCallback(getSessionUuidFuture, new FutureCallback<UUID>() {
            public void onSuccess(UUID getSessionUuidResponse) {
                responses.put("getSessionUuidResponse", getSessionUuidResponse);
                signal.countDown();
            }

            public void onFailure(Throwable error) {
                Log.e("TEST", "Error:", error);
                responses.put("getSessionUuidResponse", error);
                signal.countDown();
            }
        });

        signal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertTrue(responses.get("getSessionUuidResponse") instanceof UUID);
    }

    public void testSubscribe() throws Exception {
        final Map<String,Object> responses = new HashMap<String, Object>();

        final CountDownLatch signal = new CountDownLatch(1);

        final String testChannel = "TEST-CHANNEL";
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            @Override
            public void onMessage(PubSubMessageRecord record) {
                responses.put("error", "This should never be called for this test");
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
            new AsyncFunction<PubSubHandle, List<String>>() {
                public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                    responses.put("pubsubHandle", pubsubHandle);
                    return pubsubHandle.subscribe(testChannel, messageHandler);
                }
            };
        ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction, executor);

        AsyncFunction<List<String>, List<String>> unsubscribeFunction =
            new AsyncFunction<List<String>, List<String>>() {
                public ListenableFuture<List<String>> apply(List<String> subscribeResponse) {
                    responses.put("subscribeResponse", subscribeResponse);
                    PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
                    return pubsubHandle.unsubscribe(testChannel);
                }
            };
        ListenableFuture<List<String>> unsubscribeFuture = Futures.transformAsync(subscribeFuture, unsubscribeFunction, executor);

        Futures.addCallback(unsubscribeFuture, new FutureCallback<List<String>>() {
            public void onSuccess(List<String> unsubscribeResponse) {
                responses.put("unsubscribeResponse", unsubscribeResponse);
                signal.countDown();
            }
            public void onFailure(Throwable error) {
                Log.e("TEST","Error:",error);
                responses.put("unsubscribeResponse", error);
                signal.countDown();
            }
        });

        signal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertTrue(responses.get("error") == null);
        assertTrue(responses.get("pubsubHandle") instanceof PubSubHandle);
        assertTrue(responses.get("subscribeResponse") instanceof List);
        assertTrue(((List<String>)responses.get("subscribeResponse")).size() == 1);
        assertTrue(((List<String>)responses.get("subscribeResponse")).get(0).equals(testChannel));
        assertTrue(responses.get("unsubscribeResponse") instanceof List);
        assertTrue(((List<String>)responses.get("unsubscribeResponse")).size() == 0);
    }


    public void testListSubscriptions() throws Exception {
        final Map<String,Object> responses = new HashMap<String, Object>();
        final CountDownLatch signal = new CountDownLatch(1);

        final String testChannel = "TEST-CHANNEL";
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            @Override
            public void onMessage(PubSubMessageRecord record) {
                responses.put("error", "This should never be called for this test");
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        responses.put("pubsubHandle", pubsubHandle);
                        return pubsubHandle.subscribe(testChannel, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction, executor);

        AsyncFunction<List<String>, List<String>> listSubscriptionsFunction =
                new AsyncFunction<List<String>, List<String>>() {
                    public ListenableFuture<List<String>> apply(List<String> subscribeResponse) {
                        PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
                        return pubsubHandle.listSubscriptions();
                    }
                };
        ListenableFuture<List<String>> listSubscriptionsFuture = Futures.transformAsync(subscribeFuture, listSubscriptionsFunction, executor);

        Futures.addCallback(listSubscriptionsFuture, new FutureCallback<List<String>>() {
            public void onSuccess(List<String> listSubscriptionsResponse) {
                responses.put("listSubscriptionsResponse", listSubscriptionsResponse);
                signal.countDown();
            }
            public void onFailure(Throwable error) {
                Log.e("TEST","Error:",error);
                responses.put("listSubscriptionsResponse", error);
                signal.countDown();
            }
        });

        signal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertTrue(responses.get("error") == null);
        assertTrue(responses.get("listSubscriptionsResponse") instanceof List);
        assertTrue(((List<String>)responses.get("listSubscriptionsResponse")).size() == 1);
        assertTrue(((List<String>)responses.get("listSubscriptionsResponse")).get(0).equals(testChannel));
    }


    public void testUnsubscribeAll() throws Exception {
        final Map<String,Object> responses = new HashMap<String, Object>();
        final CountDownLatch signal = new CountDownLatch(1);

        final String testChannel = "TEST-CHANNEL";
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            @Override
            public void onMessage(PubSubMessageRecord record) {
                responses.put("error", "This should never be called for this test");
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        responses.put("pubsubHandle", pubsubHandle);
                        return pubsubHandle.subscribe(testChannel, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction, executor);

        AsyncFunction<List<String>, List<String>> unsubscribeAllFunction =
                new AsyncFunction<List<String>, List<String>>() {
                    public ListenableFuture<List<String>> apply(List<String> subscribeResponse) {
                        PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
                        return pubsubHandle.unsubscribeAll();
                    }
                };
        ListenableFuture<List<String>> unsubscribeAllFuture = Futures.transformAsync(subscribeFuture, unsubscribeAllFunction, executor);

        AsyncFunction<List<String>, List<String>> listSubscriptionsFunction =
                new AsyncFunction<List<String>, List<String>>() {
                    public ListenableFuture<List<String>> apply(List<String> unsubscribeAllResponse) {
                        responses.put("unsubscribeAllResponse", unsubscribeAllResponse);
                        PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
                        return pubsubHandle.listSubscriptions();
                    }
                };
        ListenableFuture<List<String>> listSubscriptionsFuture = Futures.transformAsync(unsubscribeAllFuture, listSubscriptionsFunction, executor);

        Futures.addCallback(listSubscriptionsFuture, new FutureCallback<List<String>>() {
            public void onSuccess(List<String> listSubscriptionsResponse) {
                responses.put("listSubscriptionsResponse", listSubscriptionsResponse);
                signal.countDown();
            }
            public void onFailure(Throwable error) {
                Log.e("TEST","Error:",error);
                responses.put("listSubscriptionsResponse", error);
                signal.countDown();
            }
        });

        signal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertNull(responses.get("error"));
        assertTrue(responses.get("unsubscribeAllResponse") instanceof List);
        assertEquals(1, ((List<String>)responses.get("unsubscribeAllResponse")).size());
        assertEquals(testChannel, ((List<String>)responses.get("unsubscribeAllResponse")).get(0));
        assertTrue(responses.get("listSubscriptionsResponse") instanceof List);
        assertEquals(0, ((List<String>)responses.get("listSubscriptionsResponse")).size());
    }


    public void testSubscribeThenPublishWithoutAck() throws Exception {
        final Map<String,Object> responses = new HashMap<String, Object>();

        final CountDownLatch signal = new CountDownLatch(1);
        final CountDownLatch subscribeMessageSignal = new CountDownLatch(1);

        final String testChannel = "TEST-CHANNEL";
        final String testMessage = "TEST-MESSAGE:"+System.currentTimeMillis()+"-"+Math.random();
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            @Override
            public void onMessage(PubSubMessageRecord record) {
                responses.put("subscribeReceivedMessage", record);
                subscribeMessageSignal.countDown();
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        responses.put("pubsubHandle", pubsubHandle);
                        return pubsubHandle.subscribe(testChannel, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction, executor);

        AsyncFunction<List<String>, Long> publishFunction =
                new AsyncFunction<List<String>, Long>() {
                    public ListenableFuture<Long> apply(List<String> subscribeResponse) {
                        responses.put("subscribeResponse", subscribeResponse);
                        PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
                        return pubsubHandle.publish(testChannel, testMessage);
                    }
                };
        ListenableFuture<Long> publishFuture = Futures.transformAsync(subscribeFuture, publishFunction, executor);

        Futures.addCallback(publishFuture, new FutureCallback<Long>() {
            public void onSuccess(Long publishResponse) {
                responses.put("publishResponse", publishResponse);
                signal.countDown();
            }
            public void onFailure(Throwable error) {
                Log.e("TEST","Error:", error);
                responses.put("publishResponse", error);
                signal.countDown();
            }
        }, executor);

        signal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertTrue(responses.get("error") == null);
        assertTrue(responses.get("publishResponse") instanceof Long);

        subscribeMessageSignal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertTrue(responses.get("subscribeReceivedMessage") instanceof PubSubMessageRecord);
        assertEquals(testMessage, ((PubSubMessageRecord)responses.get("subscribeReceivedMessage")).getMessage());
    }


    public void testSubscribeThenPublishWithAck() throws Exception {
        final Map<String,Object> responses = new HashMap<String, Object>();

        final CountDownLatch signal = new CountDownLatch(1);
        final CountDownLatch subscribeMessageSignal = new CountDownLatch(1);

        final String testChannel = "TEST-CHANNEL";
        final String testMessage = "TEST-MESSAGE:"+System.currentTimeMillis()+"-"+Math.random();
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            @Override
            public void onMessage(PubSubMessageRecord record) {
                responses.put("subscribeReceivedMessage", record);
                subscribeMessageSignal.countDown();
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        responses.put("pubsubHandle", pubsubHandle);
                        return pubsubHandle.subscribe(testChannel, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction, executor);

        AsyncFunction<List<String>, UUID> publishWithAckFunction =
                new AsyncFunction<List<String>, UUID>() {
                    public ListenableFuture<UUID> apply(List<String> subscribeResponse) {
                        responses.put("subscribeResponse", subscribeResponse);
                        PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
                        return pubsubHandle.publishWithAck(testChannel, testMessage);
                    }
                };
        ListenableFuture<UUID> publishWithAckFuture = Futures.transformAsync(subscribeFuture, publishWithAckFunction, executor);

        Futures.addCallback(publishWithAckFuture, new FutureCallback<UUID>() {
            public void onSuccess(UUID publishWithAckResponse) {
                responses.put("publishWithAckResponse", publishWithAckResponse);
                signal.countDown();
            }
            public void onFailure(Throwable error) {
                Log.e("TEST","Error:", error);
                responses.put("publishWithAckResponse", error);
                signal.countDown();
            }
        }, executor);

        signal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertTrue(responses.get("error") == null);
        assertTrue(responses.get("publishWithAckResponse") instanceof UUID);

        subscribeMessageSignal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertTrue(responses.get("subscribeReceivedMessage") instanceof PubSubMessageRecord);
        assertEquals(testMessage, ((PubSubMessageRecord)responses.get("subscribeReceivedMessage")).getMessage());
    }

    public void testClose() throws Exception {
        final Map<String,Object> responses = new HashMap<String, Object>();

        final CountDownLatch signal = new CountDownLatch(1);

        final String testChannel = "TEST-CHANNEL";
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            @Override
            public void onMessage(PubSubMessageRecord record) {
                responses.put("error", "This should never be called for this test");
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        responses.put("pubsubHandle", pubsubHandle);
                        return pubsubHandle.subscribe(testChannel, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction, executor);

        AsyncFunction<List<String>, List<String>> closeFunction =
                new AsyncFunction<List<String>, List<String>>() {
                    public ListenableFuture<List<String>> apply(List<String> subscribeResponse) {
                        PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
                        return pubsubHandle.close();
                    }
                };
        ListenableFuture<List<String>> closeFuture = Futures.transformAsync(subscribeFuture, closeFunction, executor);

        Futures.addCallback(closeFuture, new FutureCallback<List<String>>() {
            public void onSuccess(List<String> closeResponse) {
                responses.put("closeResponse", closeResponse);
                signal.countDown();
            }
            public void onFailure(Throwable error) {
                Log.e("TEST","Error:",error);
                responses.put("closeResponse", error);
                signal.countDown();
            }
        });

        signal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertNull(responses.get("error"));
        assertTrue(responses.get("closeResponse") instanceof List);
        assertEquals(1, ((List<String>)responses.get("closeResponse")).size());
        assertEquals(testChannel, ((List<String>)responses.get("closeResponse")).get(0));
    }

    public void testRestoreSession() throws Exception {
        final Map<String,Object> responses = new HashMap<String, Object>();
        final CountDownLatch signal = new CountDownLatch(1);

        final String testChannel = "TEST-CHANNEL";
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            @Override
            public void onMessage(PubSubMessageRecord record) {
                responses.put("error", "This should never be called for this test");
            }
        };

        // Open a connection, subscribe, then close.
        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        responses.put("pubsubHandle", pubsubHandle);
                        return pubsubHandle.subscribe(testChannel, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction, executor);

        AsyncFunction<List<String>, UUID> getSessionUuidFunction =
                new AsyncFunction<List<String>, UUID>() {
                    public ListenableFuture<UUID> apply(List<String> subscribeResponse) {
                        responses.put("subscribeResponse", subscribeResponse);
                        PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
                        return pubsubHandle.getSessionUuid();
                    }
                };
        ListenableFuture<UUID> getSessionUuidFuture = Futures.transformAsync(subscribeFuture, getSessionUuidFunction, executor);

        Function<UUID, List<String>> closeFunction =
                new Function<UUID, List<String>>() {
                    public List<String> apply(UUID getSessionUuidResponse) {
                        responses.put("getSessionUuidResponse", getSessionUuidResponse);
                        PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
                        pubsubHandle.dropConnection();
                        return null;
                    }
                };
        ListenableFuture<List<String>> closeFuture = Futures.transform(getSessionUuidFuture, closeFunction, executor);

        AsyncFunction<List<String>, PubSubHandle> reconnectFunction =
                new AsyncFunction<List<String>, PubSubHandle>() {
                    public ListenableFuture<PubSubHandle> apply(List<String> subscribeResponse) {
                        UUID getSessionUuidResponse = (UUID) responses.get("getSessionUuidResponse");
                        return PubSubSDK.getInstance().connect(keys, new PubSubOptions(host, false, 3000L, getSessionUuidResponse));
                    }
                };
        ListenableFuture<PubSubHandle> reconnectFuture = Futures.transformAsync(closeFuture, reconnectFunction, executor);

        AsyncFunction<PubSubHandle, List<String>> listSubscriptionsFunction =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        return pubsubHandle.listSubscriptions();
                    }
                };
        ListenableFuture<List<String>> listSubscriptionsFuture = Futures.transformAsync(reconnectFuture, listSubscriptionsFunction, executor);

        Futures.addCallback(listSubscriptionsFuture, new FutureCallback<List<String>>() {
            public void onSuccess(List<String> listSubscriptionsFuture) {
                responses.put("listSubscriptionsFuture", listSubscriptionsFuture);
                signal.countDown();
            }
            public void onFailure(Throwable error) {
                responses.put("listSubscriptionsFuture", error);
                signal.countDown();
            }
        });

        signal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertNull(responses.get("error"));
        assertEquals(testChannel, ((List<String>)responses.get("subscribeResponse")).get(0));

        assertTrue(responses.get("listSubscriptionsFuture") instanceof List);
        assertEquals(1, ((List<String>)responses.get("listSubscriptionsFuture")).size());
        assertEquals(testChannel, ((List<String>)responses.get("listSubscriptionsFuture")).get(0));
    }


    public void testSubscribeToAThenPublishToB() throws Exception {
        final Map<String,Object> responses = new HashMap<String, Object>();

        final CountDownLatch publishMessageSignal = new CountDownLatch(1);
        final CountDownLatch subscribeMessageSignal = new CountDownLatch(1);

        final String testChannelA = "TEST-CHANNEL-A";
        final String testChannelB = "TEST-CHANNEL-B";
        final String testMessage = "TEST-MESSAGE:"+System.currentTimeMillis()+"-"+Math.random();
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            @Override
            public void onMessage(PubSubMessageRecord record) {
                responses.put("subscribeReceivedMessage", record);
                subscribeMessageSignal.countDown();
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        responses.put("pubsubHandle", pubsubHandle);
                        return pubsubHandle.subscribe(testChannelA, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction, executor);

        AsyncFunction<List<String>, Long> publishFunction =
                new AsyncFunction<List<String>, Long>() {
                    public ListenableFuture<Long> apply(List<String> subscribeResponse) {
                        responses.put("subscribeResponse", subscribeResponse);
                        PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
                        return pubsubHandle.publish(testChannelB, testMessage);
                    }
                };
        ListenableFuture<Long> publishFuture = Futures.transformAsync(subscribeFuture, publishFunction, executor);

        Futures.addCallback(publishFuture, new FutureCallback<Long>() {
            public void onSuccess(Long publishResponse) {
                responses.put("publishResponse", publishResponse);
                publishMessageSignal.countDown();
            }
            public void onFailure(Throwable error) {
                Log.e("TEST","Error:", error);
                responses.put("publishResponse", error);
                publishMessageSignal.countDown();
            }
        }, executor);

        publishMessageSignal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertTrue(responses.get("error") == null);
        assertTrue(responses.get("publishResponse") instanceof Long);

        try {
            subscribeMessageSignal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);
            // If no exception is thrown, this is a failure.
            assertTrue("Expected a timeout exception, but instead received a message.", false);
        } catch (InterruptedException e) {
            // Success!  An exception is expected.
        }
    }


    public void testSubscribeToAAndBThenPublishToAndB() throws Exception {
        final Map<String,Object> responses = new HashMap<String, Object>();

        final CountDownLatch subscribeMessageSignalA = new CountDownLatch(1);
        final CountDownLatch subscribeMessageSignalB = new CountDownLatch(1);

        final String testChannelA = "TEST-CHANNEL-A";
        final String testChannelB = "TEST-CHANNEL-B";
        final String testMessageA = "TEST-MESSAGE-A:"+System.currentTimeMillis()+"-"+Math.random();
        final String testMessageB = "TEST-MESSAGE-B:"+System.currentTimeMillis()+"-"+Math.random();

        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            @Override
            public void onMessage(PubSubMessageRecord record) {
                responses.put("subscribeReceivedMessage", record);
                if (record.getMessage().contains("TEST-MESSAGE-A")) {
                    subscribeMessageSignalA.countDown();
                }
                if (record.getMessage().contains("TEST-MESSAGE-B")) {
                    subscribeMessageSignalB.countDown();
                }
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunctionA =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        responses.put("pubsubHandle", pubsubHandle);
                        return pubsubHandle.subscribe(testChannelA, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFutureA = Futures.transformAsync(connectFuture, subscribeFunctionA, executor);

        AsyncFunction<List<String>, List<String>> subscribeFunctionB =
                new AsyncFunction<List<String>, List<String> >() {
                    public ListenableFuture<List<String>> apply(List<String> subscribeResponse) {
                        responses.put("subscribeResponse", subscribeResponse);
                        PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
                        return pubsubHandle.subscribe(testChannelB, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFutureB = Futures.transformAsync(subscribeFutureA, subscribeFunctionB, executor);

        AsyncFunction<List<String>, Long> publishFunctionA =
                new AsyncFunction<List<String>, Long>() {
                    public ListenableFuture<Long> apply(List<String> subscribeResponse) {
                        responses.put("subscribeResponse", subscribeResponse);
                        PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
                        return pubsubHandle.publish(testChannelA, testMessageA);
                    }
                };
        ListenableFuture<Long> publishFutureA = Futures.transformAsync(subscribeFutureB, publishFunctionA, executor);

        AsyncFunction<Long, Long> publishFunctionB =
                new AsyncFunction<Long, Long>() {
                    public ListenableFuture<Long> apply(Long publishResponse) {
                        responses.put("publishResponse", publishResponse);
                        PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
                        return pubsubHandle.publish(testChannelA, testMessageA);
                    }
                };
        ListenableFuture<Long> publishFutureB = Futures.transformAsync(publishFutureA, publishFunctionB, executor);

        Futures.addCallback(publishFutureB, new FutureCallback<Long>() {
            public void onSuccess(Long publishResponse) {
                responses.put("publishResponse", publishResponse);
            }
            public void onFailure(Throwable error) {
                Log.e("TEST","Error:", error);
                responses.put("publishResponse", error);
            }
        }, executor);

        subscribeMessageSignalA.await(asyncTimeoutSeconds, TimeUnit.SECONDS);
        subscribeMessageSignalB.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        // Success!  Both messages received.
    }


    public void testSubscribeToAAndBThenPublishToAndBIn4Clients() throws Exception {
        final Map<String,Object> responses = new HashMap<String, Object>();

        final CountDownLatch subscribeMessageSignalA = new CountDownLatch(1);
        final CountDownLatch subscribeMessageSignalB = new CountDownLatch(1);

        final String testChannelA = "TEST-CHANNEL-A";
        final String testChannelB = "TEST-CHANNEL-B";
        final String testMessageA = "TEST-MESSAGE-A:"+System.currentTimeMillis()+"-"+Math.random();
        final String testMessageB = "TEST-MESSAGE-B:"+System.currentTimeMillis()+"-"+Math.random();

        final PubSubMessageHandler messageHandlerA = new PubSubMessageHandler() {
            @Override
            public void onMessage(PubSubMessageRecord record) {
                responses.put("subscribeReceivedMessage", record);
                subscribeMessageSignalA.countDown();
            }
        };
        final PubSubMessageHandler messageHandlerB = new PubSubMessageHandler() {
            @Override
            public void onMessage(PubSubMessageRecord record) {
                responses.put("subscribeReceivedMessage", record);
                subscribeMessageSignalB.countDown();
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunctionA =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        responses.put("pubsubHandle", pubsubHandle);
                        return pubsubHandle.subscribe(testChannelA, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFutureA = Futures.transformAsync(connectFuture, subscribeFunctionA, executor);

        AsyncFunction<List<String>, List<String>> subscribeFunctionB =
                new AsyncFunction<List<String>, List<String> >() {
                    public ListenableFuture<List<String>> apply(List<String> subscribeResponse) {
                        responses.put("subscribeResponse", subscribeResponse);
                        PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
                        return pubsubHandle.subscribe(testChannelB, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFutureB = Futures.transformAsync(subscribeFutureA, subscribeFunctionB, executor);

        AsyncFunction<List<String>, Long> publishFunctionA =
                new AsyncFunction<List<String>, Long>() {
                    public ListenableFuture<Long> apply(List<String> subscribeResponse) {
                        responses.put("subscribeResponse", subscribeResponse);
                        PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
                        return pubsubHandle.publish(testChannelA, testMessageA);
                    }
                };
        ListenableFuture<Long> publishFutureA = Futures.transformAsync(subscribeFutureB, publishFunctionA, executor);

        AsyncFunction<Long, Long> publishFunctionB =
                new AsyncFunction<Long, Long>() {
                    public ListenableFuture<Long> apply(Long publishResponse) {
                        responses.put("publishResponse", publishResponse);
                        PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
                        return pubsubHandle.publish(testChannelA, testMessageA);
                    }
                };
        ListenableFuture<Long> publishFutureB = Futures.transformAsync(publishFutureA, publishFunctionB, executor);

        Futures.addCallback(publishFutureB, new FutureCallback<Long>() {
            public void onSuccess(Long publishResponse) {
                responses.put("publishResponse", publishResponse);
            }
            public void onFailure(Throwable error) {
                Log.e("TEST","Error:", error);
                responses.put("publishResponse", error);
            }
        }, executor);

        subscribeMessageSignalA.await(asyncTimeoutSeconds, TimeUnit.SECONDS);
        subscribeMessageSignalB.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        // Success!  Both messages received.
    }

}