package io.cogswell.sdk.pubsub;

import android.util.Log;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import junit.framework.TestCase;

import org.json.JSONObject;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.cogswell.sdk.pubsub.handlers.PubSubCloseHandler;
import io.cogswell.sdk.pubsub.handlers.PubSubMessageHandler;
import io.cogswell.sdk.pubsub.handlers.PubSubNewSessionHandler;
import io.cogswell.sdk.pubsub.handlers.PubSubRawRecordHandler;
import io.cogswell.sdk.pubsub.handlers.PubSubReconnectHandler;
import io.cogswell.sdk.utils.Container;
import io.cogswell.sdk.utils.Duration;

public class PubSubHandleTest extends TestCase {
    private static int asyncTimeoutSeconds = 4;

    private Executor executor = Executors.newFixedThreadPool(16);

    private LinkedList<PubSubHandle> handles = new LinkedList<>();
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

    @Override
    protected void tearDown() throws Exception {
        final CountDownLatch latch = new CountDownLatch(handles.size());

        // Shutdown all PubSubHandles which have been connected.
        for (PubSubHandle handle : handles) {
            handle.close().addListener(new Runnable(){
                @Override
                public void run() {
                    latch.countDown();
                }
            }, executor);
        }

        latch.await(asyncTimeoutSeconds, TimeUnit.SECONDS);
    }

    public PubSubHandle stashHandle(PubSubHandle handle) {
        handles.push(handle);

        return handle;
    }

    public void testConnect() throws Exception {
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>(1);

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        assertNotNull(connectFuture);

        Futures.addCallback(connectFuture, new FutureCallback<PubSubHandle>() {
            public void onSuccess(PubSubHandle psh) {
                stashHandle(psh);
                queue.offer(psh == null ? "null-pubsub-handle" : "success");
            }
            public void onFailure(Throwable error) {
                queue.offer("connect-error");
            }
        });

        assertEquals("success", queue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
    }

    public void testGetSessionUuid() throws Exception {
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>();

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, UUID> getSessionUuidFunction =
                new AsyncFunction<PubSubHandle, UUID>() {
                    public ListenableFuture<UUID> apply(PubSubHandle pubsubHandle) {
                        return stashHandle(pubsubHandle).getSessionUuid();
                    }
                };
        ListenableFuture<UUID> getSessionUuidFuture = Futures.transformAsync(connectFuture, getSessionUuidFunction, executor);

        Futures.addCallback(getSessionUuidFuture, new FutureCallback<UUID>() {
            public void onSuccess(UUID sessionId) {
                queue.offer(sessionId == null ? "null-session-id" : "success");
            }

            public void onFailure(Throwable error) {
                queue.offer("session-id-fetch-failure");
            }
        });

        assertEquals("success", queue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
    }

    public void testSubscribe() throws Exception {
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        final Container<PubSubHandle> handle = new Container<>();

        final String testChannel = "TEST-CHANNEL";
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            public void onMessage(PubSubMessageRecord record) {
                queue.offer("should-not-have-received-a-message");
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
            new AsyncFunction<PubSubHandle, List<String>>() {
                public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                    return handle.set(stashHandle(pubsubHandle)).subscribe(testChannel, messageHandler);
                }
            };
        ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction, executor);

        AsyncFunction<List<String>, List<String>> unsubscribeFunction =
            new AsyncFunction<List<String>, List<String>>() {
                public ListenableFuture<List<String>> apply(List<String> subscribeResponse) {
                    if (!subscribeResponse.contains(testChannel)) {
                        queue.offer("expected-channel-not-in-subscriptions");
                    }

                    return handle.get().unsubscribe(testChannel);
                }
            };
        ListenableFuture<List<String>> unsubscribeFuture = Futures.transformAsync(subscribeFuture, unsubscribeFunction, executor);

        Futures.addCallback(unsubscribeFuture, new FutureCallback<List<String>>() {
            public void onSuccess(List<String> unsubscribeResponse) {
                queue.offer(unsubscribeResponse.isEmpty() ? "success" : "subscriptions-not-empty");
            }
            public void onFailure(Throwable error) {
                queue.offer("unsubscribe-failure");
            }
        });

        assertEquals("success", queue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
    }

    public void testListSubscriptions() throws Exception {
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        final Container<PubSubHandle> handle = new Container<>();

        final String testChannel = "TEST-CHANNEL";
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            public void onMessage(PubSubMessageRecord record) {
                queue.offer("should-not-have-received-a-message");
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        return handle.set(stashHandle(pubsubHandle)).subscribe(testChannel, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction, executor);

        AsyncFunction<List<String>, List<String>> listSubscriptionsFunction =
                new AsyncFunction<List<String>, List<String>>() {
                    public ListenableFuture<List<String>> apply(List<String> subscribeResponse) {
                        if (!subscribeResponse.contains(testChannel)) {
                            queue.offer("expected-channel-not-in-subscriptions");
                        }

                        return handle.get().listSubscriptions();
                    }
                };
        ListenableFuture<List<String>> listSubscriptionsFuture = Futures.transformAsync(subscribeFuture, listSubscriptionsFunction, executor);

        Futures.addCallback(listSubscriptionsFuture, new FutureCallback<List<String>>() {
            public void onSuccess(List<String> subscriptions) {
                queue.offer(subscriptions.contains(testChannel) ? "success" : "channel-missing-from-subscriptions");
            }
            public void onFailure(Throwable error) {
                queue.offer("subscription-listing-failure");
            }
        });

        assertEquals("success", queue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
    }

    public void testUnsubscribeAll() throws Exception {
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        final Container<PubSubHandle> handle = new Container<>();

        final String testChannel = "TEST-CHANNEL";
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            public void onMessage(PubSubMessageRecord record) {
                queue.offer("should-not-have-received-a-message");
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        return handle.set(stashHandle(pubsubHandle)).subscribe(testChannel, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction, executor);

        AsyncFunction<List<String>, List<String>> unsubscribeAllFunction =
                new AsyncFunction<List<String>, List<String>>() {
                    public ListenableFuture<List<String>> apply(List<String> subscribeResponse) {
                        return handle.get().unsubscribeAll();
                    }
                };
        ListenableFuture<List<String>> unsubscribeAllFuture = Futures.transformAsync(subscribeFuture, unsubscribeAllFunction, executor);

        AsyncFunction<List<String>, List<String>> listSubscriptionsFunction =
                new AsyncFunction<List<String>, List<String>>() {
                    public ListenableFuture<List<String>> apply(List<String> unsubscribeAllResponse) {
                        if (!unsubscribeAllResponse.contains(testChannel)) {
                            queue.offer("expected-channel-not-in-unsubscribe-response");
                        }

                        return handle.get().listSubscriptions();
                    }
                };
        ListenableFuture<List<String>> listSubscriptionsFuture = Futures.transformAsync(unsubscribeAllFuture, listSubscriptionsFunction, executor);

        Futures.addCallback(listSubscriptionsFuture, new FutureCallback<List<String>>() {
            public void onSuccess(List<String> subscriptions) {
                queue.offer(subscriptions.isEmpty() ? "success" : "still-subscribed-after-unsubscribe-all");
            }
            public void onFailure(Throwable error) {
                queue.offer("subscription-listing-failure");
            }
        });

        assertEquals("success", queue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
    }

    public void testSubscribeThenPublishWithoutAck() throws Exception {
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        final BlockingQueue<PubSubMessageRecord> messageQueue = new LinkedBlockingQueue<>();
        final Container<PubSubHandle> handle = new Container<>();

        final String testChannel = "TEST-CHANNEL";
        final String testMessage = "TEST-MESSAGE:"+System.currentTimeMillis()+"-"+Math.random();
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            public void onMessage(PubSubMessageRecord record) {
                messageQueue.offer(record);
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        return handle.set(stashHandle(pubsubHandle)).subscribe(testChannel, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction, executor);

        AsyncFunction<List<String>, Long> publishFunction =
                new AsyncFunction<List<String>, Long>() {
                    public ListenableFuture<Long> apply(List<String> subscribeResponse) {
                        return handle.get().publish(testChannel, testMessage);
                    }
                };
        ListenableFuture<Long> publishFuture = Futures.transformAsync(subscribeFuture, publishFunction, executor);

        Futures.addCallback(publishFuture, new FutureCallback<Long>() {
            public void onSuccess(Long publishResponse) {
                queue.offer(publishResponse == null ? "null-publish-response" : "success");
            }
            public void onFailure(Throwable error) {
                queue.offer("publish-failure");
            }
        }, executor);

        assertEquals("success", queue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));

        PubSubMessageRecord messageRecord = messageQueue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS);
        assertEquals(testMessage, messageRecord.getMessage());
        assertEquals(testChannel, messageRecord.getChannel());
    }

    public void testSubscribeThenPublishWithAck() throws Exception {
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        final BlockingQueue<PubSubMessageRecord> messageQueue = new LinkedBlockingQueue<>();
        final Container<PubSubHandle> handle = new Container<>();

        final String testChannel = "TEST-CHANNEL";
        final String testMessage = "TEST-MESSAGE:"+System.currentTimeMillis()+"-"+Math.random();
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            public void onMessage(PubSubMessageRecord record) {
                messageQueue.offer(record);
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        return handle.set(stashHandle(pubsubHandle)).subscribe(testChannel, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction, executor);

        AsyncFunction<List<String>, UUID> publishWithAckFunction =
                new AsyncFunction<List<String>, UUID>() {
                    public ListenableFuture<UUID> apply(List<String> subscribeResponse) {
                        return handle.get().publishWithAck(testChannel, testMessage);
                    }
                };
        ListenableFuture<UUID> publishWithAckFuture = Futures.transformAsync(subscribeFuture, publishWithAckFunction, executor);

        Futures.addCallback(publishWithAckFuture, new FutureCallback<UUID>() {
            public void onSuccess(UUID publishWithAckResponse) {
                queue.offer(publishWithAckResponse == null ? "null-message-id-response" : "success");
            }
            public void onFailure(Throwable error) {
                queue.offer("publish-with-ack-failure");
            }
        }, executor);

        assertEquals("success", queue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
        assertEquals(testMessage, messageQueue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS).getMessage());
    }

    public void testClose() throws Exception {
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        final Container<PubSubHandle> handle = new Container<>();

        final String testChannel = "TEST-CHANNEL";
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            public void onMessage(PubSubMessageRecord record) {
                queue.offer("should-not-have-received-a-message");
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        return handle.set(stashHandle(pubsubHandle)).subscribe(testChannel, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction, executor);

        AsyncFunction<List<String>, Void> closeFunction =
                new AsyncFunction<List<String>, Void>() {
                    public ListenableFuture<Void> apply(List<String> subscribeResponse) {
                        return handle.get().close();
                    }
                };
        ListenableFuture<Void> closeFuture = Futures.transformAsync(subscribeFuture, closeFunction, executor);

        Futures.addCallback(closeFuture, new FutureCallback<Void>() {
            public void onSuccess(Void closeResponse) {
                queue.offer("success");
            }
            public void onFailure(Throwable error) {
                queue.offer("socket-close-failure");
            }
        });

        assertEquals("success", queue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
    }

    public void testRestoreSession() throws Exception {
        final Container<PubSubHandle> firstHandle = new Container<>();
        final Container<PubSubHandle> secondHandle = new Container<>();
        final Container<UUID> uuid = new Container<>();
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        final Container<SortedSet<String>> originalSubscriptions = new Container<>();
        final Container<SortedSet<String>> reconnectSubscriptions = new Container<>();

        final String testChannel = "TEST-CHANNEL";
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            public void onMessage(PubSubMessageRecord record) {
                queue.offer("should-not-have-received-a-message");
            }
        };

        // Open a connection, subscribe, then close.
        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        return firstHandle.set(stashHandle(pubsubHandle)).subscribe(testChannel, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction, executor);

        AsyncFunction<List<String>, UUID> getSessionUuidFunction =
                new AsyncFunction<List<String>, UUID>() {
                    public ListenableFuture<UUID> apply(List<String> subscriptions) {
                        originalSubscriptions.set(new TreeSet<>(subscriptions));
                        return firstHandle.get().getSessionUuid();
                    }
                };
        ListenableFuture<UUID> getSessionUuidFuture = Futures.transformAsync(subscribeFuture, getSessionUuidFunction, executor);

        Function<UUID, List<String>> closeFunction =
                new Function<UUID, List<String>>() {
                    public List<String> apply(UUID sessionId) {
                        uuid.set(sessionId);
                        firstHandle.get().dropConnection(new PubSubDropConnectionOptions(Duration.of(10, TimeUnit.MILLISECONDS)));
                        return null;
                    }
                };
        ListenableFuture<List<String>> closeFuture = Futures.transform(getSessionUuidFuture, closeFunction, executor);

        AsyncFunction<List<String>, PubSubHandle> reconnectFunction =
                new AsyncFunction<List<String>, PubSubHandle>() {
                    public ListenableFuture<PubSubHandle> apply(List<String> subscriptions) {
                        return PubSubSDK.getInstance().connect(keys, new PubSubOptions(host, false, Duration.of(3, TimeUnit.SECONDS), uuid.get()));
                    }
                };
        ListenableFuture<PubSubHandle> reconnectFuture = Futures.transformAsync(closeFuture, reconnectFunction, executor);

        AsyncFunction<PubSubHandle, List<String>> listSubscriptionsFunction =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        return secondHandle.set(stashHandle(pubsubHandle)).listSubscriptions();
                    }
                };
        ListenableFuture<List<String>> listSubscriptionsFuture = Futures.transformAsync(reconnectFuture, listSubscriptionsFunction, executor);

        Futures.addCallback(listSubscriptionsFuture, new FutureCallback<List<String>>() {
            public void onSuccess(List<String> subscriptions) {
                reconnectSubscriptions.set(new TreeSet<>(subscriptions));
                queue.offer(subscriptions.contains(testChannel) ? "success" : "channel-missing-from-subscriptions");
            }
            public void onFailure(Throwable error) {
                queue.offer("subscription-listing-failure");
            }
        });

        assertEquals("success", queue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
        assertEquals(originalSubscriptions.get(), reconnectSubscriptions.get());
    }

    public void testSubscribeToAThenPublishToB() throws Exception {
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        final BlockingQueue<PubSubMessageRecord> messageQueue = new LinkedBlockingQueue<>();
        final Container<PubSubHandle> handle = new Container<>();

        final String testChannelA = "TEST-CHANNEL-A";
        final String testChannelB = "TEST-CHANNEL-B";
        final String testMessage = "TEST-MESSAGE:"+System.currentTimeMillis()+"-"+Math.random();
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            public void onMessage(PubSubMessageRecord record) {
                messageQueue.offer(record);
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        return handle.set(stashHandle(pubsubHandle)).subscribe(testChannelA, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction, executor);

        AsyncFunction<List<String>, Long> publishFunction =
                new AsyncFunction<List<String>, Long>() {
                    public ListenableFuture<Long> apply(List<String> subscribeResponse) {
                        return handle.get().publish(testChannelB, testMessage);
                    }
                };
        ListenableFuture<Long> publishFuture = Futures.transformAsync(subscribeFuture, publishFunction, executor);

        Futures.addCallback(publishFuture, new FutureCallback<Long>() {
            public void onSuccess(Long publishResponse) {
                queue.offer(publishResponse == null ? "null-publish-response" : "success");
            }
            public void onFailure(Throwable error) {
                queue.offer("publish-failure");
            }
        }, executor);

        assertEquals("success", queue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
        assertNull(messageQueue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
    }


    public void testSubscribeToAAndBThenPublishToAndB() throws Exception {
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        final BlockingQueue<PubSubMessageRecord> messageQueueA = new LinkedBlockingQueue<>();
        final BlockingQueue<PubSubMessageRecord> messageQueueB = new LinkedBlockingQueue<>();
        final Container<PubSubHandle> handle = new Container<>();

        final String testChannelA = "TEST-CHANNEL-A";
        final String testChannelB = "TEST-CHANNEL-B";
        final String testMessageA = "TEST-MESSAGE-A:"+System.currentTimeMillis()+"-"+Math.random();
        final String testMessageB = "TEST-MESSAGE-B:"+System.currentTimeMillis()+"-"+Math.random();

        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            public void onMessage(PubSubMessageRecord record) {
                String channel = record.getChannel();

                if (testChannelA.equals(channel)) {
                    messageQueueA.offer(record);
                } else if (testChannelB.equals(channel)) {
                    messageQueueB.offer(record);
                }
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        AsyncFunction<PubSubHandle, List<String>> subscribeFunctionA =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        return handle.set(stashHandle(pubsubHandle)).subscribe(testChannelA, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFutureA = Futures.transformAsync(connectFuture, subscribeFunctionA, executor);

        AsyncFunction<List<String>, List<String>> subscribeFunctionB =
                new AsyncFunction<List<String>, List<String> >() {
                    public ListenableFuture<List<String>> apply(List<String> subscribeResponse) {
                        return handle.get().subscribe(testChannelB, messageHandler);
                    }
                };
        ListenableFuture<List<String>> subscribeFutureB = Futures.transformAsync(subscribeFutureA, subscribeFunctionB, executor);

        AsyncFunction<List<String>, Long> publishFunctionA =
                new AsyncFunction<List<String>, Long>() {
                    public ListenableFuture<Long> apply(List<String> subscribeResponse) {
                        return handle.get().publish(testChannelA, testMessageA);
                    }
                };
        ListenableFuture<Long> publishFutureA = Futures.transformAsync(subscribeFutureB, publishFunctionA, executor);

        AsyncFunction<Long, Long> publishFunctionB =
                new AsyncFunction<Long, Long>() {
                    public ListenableFuture<Long> apply(Long publishResponse) {
                        return handle.get().publish(testChannelB, testMessageB);
                    }
                };
        ListenableFuture<Long> publishFutureB = Futures.transformAsync(publishFutureA, publishFunctionB, executor);

        Futures.addCallback(publishFutureB, new FutureCallback<Long>() {
            public void onSuccess(Long publishResponse) {
                queue.offer(publishResponse == null ? "null-publish-response" : "success");
            }
            public void onFailure(Throwable error) {
                queue.offer("publish-failure");
            }
        }, executor);

        assertEquals("success", queue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));

        PubSubMessageRecord recordA = messageQueueA.poll(asyncTimeoutSeconds, TimeUnit.SECONDS);
        assertNotNull(recordA);
        assertEquals(testMessageA, recordA.getMessage());
        assertEquals(testChannelA, recordA.getChannel());

        PubSubMessageRecord recordB = messageQueueB.poll(asyncTimeoutSeconds, TimeUnit.SECONDS);
        assertNotNull(recordB);
        assertEquals(testMessageB, recordB.getMessage());
        assertEquals(testChannelB, recordB.getChannel());

        // Success!  Both messages received.
    }

    public void testSubscribeToAAndBThenPublishToAndBIn4Clients() throws Exception {
        final Container<PubSubHandle> pubHandleA = new Container<>();
        final Container<PubSubHandle> pubHandleB = new Container<>();

        final Container<UUID> messageIdA = new Container<>();
        final Container<UUID> messageIdB = new Container<>();

        final CountDownLatch readyLatch = new CountDownLatch(4);

        final BlockingQueue<String> queueSubA = new LinkedBlockingQueue<>();
        final BlockingQueue<String> queueSubB = new LinkedBlockingQueue<>();
        final BlockingQueue<String> queuePubA = new LinkedBlockingQueue<>();
        final BlockingQueue<String> queuePubB = new LinkedBlockingQueue<>();
        final BlockingQueue<PubSubMessageRecord> messageQueueA = new LinkedBlockingQueue<>();
        final BlockingQueue<PubSubMessageRecord> messageQueueB = new LinkedBlockingQueue<>();

        final String testChannelA = "TEST-CHANNEL-A";
        final String testChannelB = "TEST-CHANNEL-B";
        final String testMessageA = "TEST-MESSAGE-A:"+System.currentTimeMillis()+"-"+Math.random();
        final String testMessageB = "TEST-MESSAGE-B:"+System.currentTimeMillis()+"-"+Math.random();

        final PubSubMessageHandler messageHandlerChannelA = new PubSubMessageHandler() {
            public void onMessage(PubSubMessageRecord record) {
                messageQueueA.offer(record);
            }
        };

        final PubSubMessageHandler messageHandlerChannelB = new PubSubMessageHandler() {
            public void onMessage(PubSubMessageRecord record) {
                messageQueueB.offer(record);
            }
        };

        // Subscriber A:
        ListenableFuture<PubSubHandle> connectFutureSubscriberA = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));
        AsyncFunction<PubSubHandle, List<String>> subscribeFunctionA =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        return stashHandle(pubsubHandle).subscribe(testChannelA, messageHandlerChannelA);
                    }
                };

        Futures.addCallback(
                Futures.transformAsync(connectFutureSubscriberA, subscribeFunctionA, executor),
                new FutureCallback<List<String>>() {
                    public void onSuccess(List<String> result) {
                        readyLatch.countDown();
                        queueSubA.offer(result.contains(testChannelA) ? "success" : "channel-A-missing-from-subscriptions");
                    }
                    public void onFailure(Throwable t) {
                        queueSubA.offer("subscription-A-failure");
                    }
                }
        );

        // Subscriber B:
        ListenableFuture<PubSubHandle> connectFutureSubscriberB = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));
        AsyncFunction<PubSubHandle, List<String>> subscribeFunctionB =
                new AsyncFunction<PubSubHandle, List<String>>() {
                    public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                        return stashHandle(pubsubHandle).subscribe(testChannelB, messageHandlerChannelB);
                    }
                };

        Futures.addCallback(
                Futures.transformAsync(connectFutureSubscriberB, subscribeFunctionB, executor),
                new FutureCallback<List<String>>() {
                    public void onSuccess(List<String> result) {
                        readyLatch.countDown();
                        queueSubB.offer(result.contains(testChannelB) ? "success" : "channel-B-missing-from-subscriptions");
                    }
                    public void onFailure(Throwable t) {
                        queueSubB.offer("subscription-B-failure");
                    }
                }
        );

        // Publisher A:
        Futures.addCallback(
                PubSubSDK.getInstance().connect(keys, new PubSubOptions(host)),
                new FutureCallback<PubSubHandle>() {
                    public void onSuccess(PubSubHandle handle) {
                        pubHandleA.set(handle);
                        readyLatch.countDown();
                    }
                    public void onFailure(Throwable t) {
                        queuePubA.offer("publisher-A-connect-failure");
                    }
                }
        );

        final Runnable publishA = new Runnable() {
            public void run() {
                Futures.addCallback(
                        pubHandleA.get().publishWithAck(testChannelA, testMessageA),
                        new FutureCallback<UUID>() {
                            public void onSuccess(UUID messageId) {
                                messageIdA.set(messageId);
                                queuePubA.offer(messageId == null ? "null-sequence-for-channel-A-publish" : "success");
                            }
                            public void onFailure(Throwable t) {
                                queuePubA.offer("publish-to-channel-A-failed");
                            }
                        }
                );
            }
        };

        // Publisher B:
        Futures.addCallback(
                PubSubSDK.getInstance().connect(keys, new PubSubOptions(host)),
                new FutureCallback<PubSubHandle>() {
                    public void onSuccess(PubSubHandle handle) {
                        pubHandleB.set(handle);
                        readyLatch.countDown();
                    }
                    public void onFailure(Throwable t) {
                        queuePubB.offer("publisher-B-connect-failure");
                    }
                }
        );

        final Runnable publishB = new Runnable() {
            public void run() {
                Futures.addCallback(
                        pubHandleB.get().publishWithAck(testChannelB, testMessageB),
                        new FutureCallback<UUID>() {
                            public void onSuccess(UUID messageId) {
                                messageIdB.set(messageId);
                                queuePubB.offer(messageId == null ? "null-message-id-for-channel-B-publish" : "success");
                            }
                            public void onFailure(Throwable t) {
                                queuePubB.offer("publish-to-channel-B-failed");
                            }
                        }
                );
            }
        };

        long asyncTimeoutSeconds = 3;

        // Wait for all connections to be established.
        readyLatch.await(asyncTimeoutSeconds, TimeUnit.SECONDS);

        executor.execute(publishA);
        executor.execute(publishB);

        assertEquals("success", queueSubA.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
        assertEquals("success", queueSubB.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
        assertEquals("success", queuePubA.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
        assertEquals("success", queuePubB.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));

        PubSubMessageRecord recordA = messageQueueA.poll(asyncTimeoutSeconds, TimeUnit.SECONDS);
        assertNotNull(recordA);
        assertEquals(testMessageA, recordA.getMessage());
        assertEquals(testChannelA, recordA.getChannel());
        assertEquals(messageIdA.get(), recordA.getId());

        PubSubMessageRecord recordB = messageQueueB.poll(asyncTimeoutSeconds, TimeUnit.SECONDS);
        assertNotNull(recordB);
        assertEquals(testMessageB, recordB.getMessage());
        assertEquals(testChannelB, recordB.getChannel());
        assertEquals(messageIdB.get(), recordB.getId());
    }

    /**
     * Test a single PubSubHandle going through all of the features in sequence.
     */
    public void testFullSweep() throws Exception {
        final Container<PubSubHandle> handle = new Container<>();
        final Container<String> failure = new Container<>();
        final Container<UUID> oldSession = new Container<>();
        final Container<UUID> newSession = new Container<>();
        final Container<UUID> replacementSession = new Container<>();
        final Container<UUID> firstMessageId = new Container<>();
        final Container<UUID> secondMessageId = new Container<>();
        final Container<SortedSet<String>> subscribeMainSubscriptions = new Container<>();
        final Container<SortedSet<String>> subscribeControlSubscriptions = new Container<>();
        final Container<SortedSet<String>> reconnectSubscriptions = new Container<>();
        final Container<SortedSet<String>> unsubscribeMainSubscriptions = new Container<>();
        final Container<SortedSet<String>> unsubscribeAllSubscriptions = new Container<>();
        final Container<PubSubMessageRecord> controlMessage = new Container<>();

        final BlockingQueue<String> reconnectQueue = new LinkedBlockingQueue<>(1);
        final BlockingQueue<String> messageDeliveredQueue = new LinkedBlockingQueue<>(1);
        final BlockingQueue<String> closeQueue = new LinkedBlockingQueue<>(1);
        final BlockingQueue<PubSubMessageRecord> messageQueue = new LinkedBlockingQueue<>(1);

        final String mainChannel = "new-channel-test";//"MAIN-TEST-CHANNEL-" + System.nanoTime();
        final String controlChannel = "CONTROL-TEST-CHANNEL-" + System.nanoTime();
        final String firstMessage = "TEST-MESSAGE-" + System.nanoTime();
        final String secondMessage = "TEST-MESSAGE-" + System.nanoTime();

        // Message handler for the main channel.
        final PubSubMessageHandler mainChannelMessageHandler = new PubSubMessageHandler() {
            public void onMessage(PubSubMessageRecord record) {
                messageQueue.offer(record);
            }
        };

        final PubSubRawRecordHandler rawRecordHandler = new PubSubRawRecordHandler() {
            public void onRawRecord(String rawRecord) {
                Log.e("MESSAGE_FOR_YOU_SIR::::", rawRecord);
            }
        };

        // Message handler for the control channel (should never receive anything).
        final PubSubMessageHandler controlChannelMessageHandler = new PubSubMessageHandler() {
            public void onMessage(PubSubMessageRecord record) {
                controlMessage.set(record);
            }
        };

        // Reconnect handler.
        final PubSubReconnectHandler reconnectHandler = new PubSubReconnectHandler() {
            public void onReconnect() {
                reconnectQueue.offer("reconnected");
            }
        };

        // New session handler.
        final PubSubNewSessionHandler newSessionHandler = new PubSubNewSessionHandler() {
            public void onNewSession(UUID uuid) {
                replacementSession.set(uuid);
            }
        };

        // Socket close handler.
        final PubSubCloseHandler closeHandler = new PubSubCloseHandler() {
            public void onClose(Throwable error) {
                closeQueue.offer("closed");
            }
        };

        // Establish the initial connection.
        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, new PubSubOptions(host));

        // Stash the handle then fetch the session UUID.
        AsyncFunction<PubSubHandle, UUID> getSessionIdTransformer = new AsyncFunction<PubSubHandle, UUID>() {
            @Override
            public ListenableFuture<UUID> apply(PubSubHandle pubsubHandle) throws Exception {
                // Stash the PubSubHandle for later user.
                handle.set(stashHandle(pubsubHandle));

                pubsubHandle.onReconnect(reconnectHandler);
                pubsubHandle.onNewSession(newSessionHandler);
                pubsubHandle.onRawRecord(rawRecordHandler);
                pubsubHandle.onClose(closeHandler);

                return pubsubHandle.getSessionUuid();
            }
        };

        ListenableFuture<UUID> oldSessionIdFuture = Futures.transformAsync(connectFuture, getSessionIdTransformer);

        // Record the old session UUID then subscribe to main channel.
        AsyncFunction<UUID, List<String>> subscribeToMainTransformer = new AsyncFunction<UUID, List<String>>() {
            public ListenableFuture<List<String>> apply(UUID sessionId) {
                oldSession.set(sessionId);

                // Subscribe to the main channel.
                return handle.get().subscribe(mainChannel, mainChannelMessageHandler);
            }
        };

        ListenableFuture<List<String>> mainSubscribeFuture = Futures.transformAsync(oldSessionIdFuture, subscribeToMainTransformer);

        // Report the subscriptions post subscribe to main, then subscribe to the control channel.
        AsyncFunction<List<String>, List<String>> subscribeToControlTransformer = new AsyncFunction<List<String>, List<String>>() {
            public ListenableFuture<List<String>> apply(List<String> subscriptions) {
                // Now subscribed to main channel.
                subscribeMainSubscriptions.set(new TreeSet<>(subscriptions));

                // Subscribe to the control channel
                return handle.get().subscribe(controlChannel, controlChannelMessageHandler);
            }
        };

        ListenableFuture<List<String>> controlSubscribeFuture = Futures.transformAsync(mainSubscribeFuture, subscribeToControlTransformer);

        // Report the subscriptions post subscribe to control, then publish the first message.
        AsyncFunction<List<String>, UUID> publishFirstMessageTransformer = new AsyncFunction<List<String>, UUID>() {
            public ListenableFuture<UUID> apply(List<String> subscriptions) throws Exception {
                // Now subscribed to control channel.
                subscribeControlSubscriptions.set(new TreeSet<>(subscriptions));

                // Publish the first message, expecting an acknowledgement.
                return handle.get().publishWithAck(mainChannel, firstMessage);
            }
        };

        ListenableFuture<UUID> publishFirstMessageFuture = Futures.transformAsync(controlSubscribeFuture, publishFirstMessageTransformer);

        // Report the subscriptions post subscribe to control, then drop the connection.
        FutureCallback<UUID> firstMessageConfirmationCallback = new FutureCallback<UUID>() {
            public void onSuccess(UUID messageId) {
                // First message was successfully delivered.
                firstMessageId.set(messageId);

                // Notify of message 1 delivery.
                messageDeliveredQueue.offer("message-1-published");
            }
            public void onFailure(Throwable t) {
                failure.set("failed-before-first-publish");
            }
        };

        Futures.addCallback(publishFirstMessageFuture, firstMessageConfirmationCallback);

        // Wait for the first message, and evaluate its contents.
        assertEquals("message-1-published", messageDeliveredQueue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
        assertNotNull(firstMessageId.get());

        PubSubMessageRecord msg1Record = messageQueue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS);
        assertNotNull(msg1Record);
        assertEquals(firstMessageId.get(), msg1Record.getId());
        assertEquals(mainChannel, msg1Record.getChannel());
        assertEquals(firstMessage, msg1Record.getMessage());

        // After receiving the first message, drop the connection.
        handle.get().dropConnection(new PubSubDropConnectionOptions(
                Duration.of(0L, TimeUnit.MICROSECONDS)
        ));

        // Once we have reconnected (reconnectHandler), fetch the new session UUID.
        assertEquals("reconnected", reconnectQueue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
        ListenableFuture<UUID> newSessionIdFuture = handle.get().getSessionUuid();

        // Record the new session UUID, then fetch the list of subscriptions to confirm that they were restored.
        AsyncFunction<UUID, List<String>> fetchSubscriptionsTransformer = new AsyncFunction<UUID, List<String>>() {
            public ListenableFuture<List<String>> apply(UUID sessionId) throws Exception {
                // Stash the reconnect session's ID.
                newSession.set(sessionId);

                // List the channels to which we are subscribed post reconnect.
                return handle.get().listSubscriptions();
            }
        };

        ListenableFuture<List<String>> restoredSubscriptionsFuture = Futures.transformAsync(newSessionIdFuture, fetchSubscriptionsTransformer);

        // Record the restored subscriptions, then publish the second message (with acknowledgement).
        AsyncFunction<List<String>, UUID> publishMessageTransformer = new AsyncFunction<List<String>, UUID>() {
            public ListenableFuture<UUID> apply(List<String> subscriptions) throws Exception {
                // Stash the post-reconnect (restored) subscriptions.
                reconnectSubscriptions.set(new TreeSet<>(subscriptions));

                // Publish the second message, expecting an acknowledgement.
                return handle.get().publishWithAck(mainChannel, secondMessage);
            }
        };

        ListenableFuture<UUID> publishedMessageIdFuture = Futures.transformAsync(restoredSubscriptionsFuture, publishMessageTransformer);

        FutureCallback<UUID> publishMessageCallback = new FutureCallback<UUID>() {
            public void onSuccess(UUID messageId) {
                // Second message was successfully delivered.
                secondMessageId.set(messageId);

                // Notify of message 2 delivery.
                messageDeliveredQueue.offer("message-2-published");
            }
            public void onFailure(Throwable t) {
                messageDeliveredQueue.offer("failed-to-publish-message");
            }
        };

        Futures.addCallback(publishedMessageIdFuture, publishMessageCallback);

        // Once the message has been published and delivered, then move on to unsubscribe operations.
        assertEquals("message-2-published", messageDeliveredQueue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));
        assertNotNull(secondMessageId.get());

        assertEquals(oldSession.get(), newSession.get());
        assertEquals(new TreeSet<>(Arrays.asList(mainChannel)), subscribeMainSubscriptions.get());
        assertEquals(new TreeSet<>(Arrays.asList(mainChannel, controlChannel)), subscribeControlSubscriptions.get());
        assertEquals(new TreeSet<>(Arrays.asList(mainChannel, controlChannel)), reconnectSubscriptions.get());

        PubSubMessageRecord msg2Record = messageQueue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS);

        assertNotNull(msg2Record);
        assertEquals(secondMessageId.get(), msg2Record.getId());
        assertEquals(mainChannel, msg2Record.getChannel());
        assertEquals(secondMessage, msg2Record.getMessage());

        // Now unsubscribe from the main channel.
        ListenableFuture<List<String>> unsubscribeMainFuture = handle.get().unsubscribe(mainChannel);

        // After recording the subscriptions post unsubscribe from main channel, unsubscribe from all channels.
        AsyncFunction<List<String>, List<String>> unsubscribeAllTransformer = new AsyncFunction<List<String>, List<String>>() {
            public ListenableFuture<List<String>> apply(List<String> subscriptions) throws Exception {
                // Stash the list of subscriptions post unsubscribe from main.
                unsubscribeMainSubscriptions.set(new TreeSet<>(subscriptions));

                // Now unsubscribe from all channels.
                return handle.get().unsubscribeAll();
            }
        };

        ListenableFuture<List<String>> unsubscribeAllFuture = Futures.transformAsync(unsubscribeMainFuture, unsubscribeAllTransformer);

        // After recording the subscriptions post unsubscribe all, close the connection.
        AsyncFunction<List<String>, Void> closeTransformer = new AsyncFunction<List<String>, Void>() {
            public ListenableFuture<Void> apply(List<String> subscriptions) throws Exception {
                // Stash the list of subscriptions post unsubscribe from all.
                unsubscribeAllSubscriptions.set(new TreeSet<>(subscriptions));

                // Now close the connection.
                return handle.get().close();
            }
        };

        assertEquals("closed", closeQueue.poll(asyncTimeoutSeconds, TimeUnit.SECONDS));

        assertNotNull(handle.get());
        assertNull(failure.get());
        assertNull(controlMessage.get());
        assertNull(replacementSession.get());

        assertEquals(new TreeSet<>(Arrays.asList(controlChannel)), unsubscribeMainSubscriptions.get());
        assertEquals(new TreeSet<>(Arrays.asList(controlChannel)), unsubscribeAllSubscriptions.get());
    }
}