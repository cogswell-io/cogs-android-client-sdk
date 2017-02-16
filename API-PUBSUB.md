# Cogswell.ioPub/Sub SDK

- [`PubSubSDK`](#pubsubsdk)
  - [`getInstance()`](#getinstance)
  - [`connect (keys)`](#connect-keys)
- [`PubSubOptions`](#pubsuboptions)
  - [`new PubSubOptions()`](#new-pubsuboptions)
  - [`new PubSubOptions (url, isAutoReconnect, connectTimeoutMs, sessionUuid)`](#new-pubsuboptions-isautoreconnect-connecttimeoutms-sessionUuid)
- [`PubSubHandle`](#pubsubhandle)
  - [`getSessionUuid()`](#setsessionuuid)
  - [`subscribe (channel, handler)`](#subscribe-channel-handler)
  - [`unsubscribe (channel)`](#unsubscribe-channel)
  - [`unsubscribeAll()`](#unsubscribeall)
  - [`listSubscriptions()`](#listsubscriptions)
  - [`publish (channel, message)`](#publish-channel-message)
  - [`publishWithAck (channel, message)`](#publishwithack-channel-message)
  - [`close()`](#close)
- [A complete example](#a-complete-example)
- [Running the tests](#running-the-tests)

## Code Examples

The code examples that follow illustrate the individual methods of the Android
Cogswell.ioPub/Sub SDK. The examples illustrate only how the methods might be
used. For more information, including complete method signatures and possible
errors, see the documentation at [the Cogswell.ioPub/Sub website](https://cogswell.io/docs/android/client-sdk/api/).

To support versions of Android as old as API level 15, this SDK uses the [Guava](https://github/google/guava) library for async calls.

[A complete example](#a-complete-example), illustrating how the methods can be used
together, is provided at the end of this document.

### [`PubSubSDK`](#pubsubsdk)

#### [`getInstance()`](#getinstance)
You can use the `getInstance` method to get a reference to the sdk.
```java
PubSubSDK sdk = PubSubSDK.getInstance();
```

#### [`connect (keys)`](#connect-keys)
You'll need to get your the from your Cogswell.io pub/sub account.
```java
List<String> keys = new ArrayList<String>();
keys.add("A-*-*");
keys.add("R-*-*");
keys.add("W-*-*");
```

```java
ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys);

Futures.addCallback(connectFuture, new FutureCallback<PubSubHandle>() {
    public void onSuccess(PubSubHandle psh) {
        Log.e("TEST","Connected");
    }
    public void onFailure(Throwable error) {
        Log.e("TEST","Error:", error);
    }
});
```

### [`PubSubOptions`](#pubsuboptions)

You can use these options to customize your connection.

#### [`new PubSubOptions()`](#new-pubsuboptions)
Constructs a default options object that connects to Cogswell.io that will auto-reconnect, retry lost connections at 30 second intervals, and create a new session.
```java
PubSubOptions options = PubSubOptions();
ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, options);
```
#### [`new PubSubOptions (url, isAutoReconnect, connectTimeoutMs, sessionUuid)`](#new-pubsuboptions-isautoreconnect-connecttimeoutms-sessionUuid)
If you are behind a proxy server, you may need to change the connection url.  You can also disable auto-reconnect, change the timeout.  If you want to create a new session, use a uuid of null.  If you are attempting to restore a session from a previous connection, pass in the the last value retrieved from [`getSessionUuid()`]()
```java
PubSubOptions options = PubSubOptions("wss://api.cogswell.io/pubsub", false, 20000L, previousSession)
ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys, options);
```

### [`PubSubHandle`](#pubsubhandle)

Use this object to make calls to the server.

#### [`getSessionUuid()`](#setsessionuuid)

Use this call to get the session id if you intend to try to restore your session after a temporary loss of your network connection.  This id is only valid for up to 5 minutes after disconnecting, so be sure to verify your id matches after reconnecting.

```java
ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys);
 
AsyncFunction<PubSubHandle, UUID> getSessionUuidFunction =
        new AsyncFunction<PubSubHandle, UUID>() {
            public ListenableFuture<UUID> apply(PubSubHandle pubsubHandle) {
                return pubsubHandle.getSessionUuid();
            }
        };
ListenableFuture<UUID> getSessionUuidFuture = Futures.transformAsync(connectFuture, getSessionUuidFunction);
 
Futures.addCallback(getSessionUuidFuture, new FutureCallback<UUID>() {
    public void onSuccess(UUID getSessionUuidResponse) {
        Log.e("TEST","UUID:", getSessionUuidResponse);
    }
    public void onFailure(Throwable error) {
        Log.e("TEST","Error:", error);
    }
});
```

#### [`subscribe (channel, handler)`](#subscribe-channel-handler)

Use this to start listening to a channel.  You'll need to specify a channel, and a handler to be called whenever a message arrives.  

Here is an example message handler - see the [docs](https://cogswell.io/docs/android/client-sdk/api/) for more about the PubSubMessageRecord object.

```java
final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
    @Override
    public void onMessage(PubSubMessageRecord record) {
        Log.e("TEST","record.getMessage():", record.getMessage());
    }
};
```


See [`unsubscribe (channel)`](#unsubscribe-channel), below, for an example of how this is used.

#### [`unsubscribe (channel)`](#unsubscribe-channel)

Use this to stop listening to a channel.  The server will respond with the list of remaining subscriptions.

```java
ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys);
final Map<String,Object> responses = new HashMap<String, Object>();
 
AsyncFunction<PubSubHandle, List<String>> subscribeFunction =
    new AsyncFunction<PubSubHandle, List<String>>() {
        public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
            responses.put("pubsubHandle", pubsubHandle);
            return pubsubHandle.subscribe(testChannel, messageHandler);
        }
    };
ListenableFuture<List<String>> subscribeFuture = Futures.transformAsync(connectFuture, subscribeFunction);
 
AsyncFunction<List<String>, List<String>> unsubscribeFunction =
    new AsyncFunction<List<String>, List<String>>() {
        public ListenableFuture<List<String>> apply(List<String> subscribeResponse) {
            responses.put("subscribeResponse", subscribeResponse);
            PubSubHandle pubsubHandle = (PubSubHandle) responses.get("pubsubHandle");
            return pubsubHandle.unsubscribe(testChannel);
        }
    };
ListenableFuture<List<String>> unsubscribeFuture = Futures.transformAsync(subscribeFuture, unsubscribeFunction);
 
Futures.addCallback(unsubscribeFuture, new FutureCallback<List<String>>() {
    public void onSuccess(List<String> unsubscribeResponse) {
        responses.put("unsubscribeResponse", unsubscribeResponse);
    }
    public void onFailure(Throwable error) {
        Log.e("TEST","Error:",error);
    }
});
```

#### [`unsubscribeAll()`](#unsubscribeall)

You can unsubscribe from all channels without specifying any.  Unlike `unsubscribe(channel)`, this will return the list of channels you unsubscribed to.

```java
pubsubHandle.unsubscribeAll();
```

#### [`listSubscriptions()`](#listsubscriptions)
You can list the channels you are subscribed to.

```java
ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys);
 
AsyncFunction<PubSubHandle, List<String>> listSubscriptionsFunction =
        new AsyncFunction<PubSubHandle, List<String>>() {
            public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                return pubsubHandle.listSubscriptions();
            }
        };
ListenableFuture<List<String>> listSubscriptionsFuture = Futures.transformAsync(connectFuture, listSubscriptionsFunction);
 
Futures.addCallback(listSubscriptionsFuture, new FutureCallback<List<String>>() {
    public void onSuccess(List<String> listSubscriptionsResponse) {
        Log.e("TEST","listSubscriptionsResponse:", listSubscriptionsResponse);
    }
    public void onFailure(Throwable error) {
        Log.e("TEST","Error:", error);
    }
});
```

#### [`publish (channel, message)`](publish-channel-message)

Use this to publish to any channel.  This call will complete immediately with a client side sequence number, unique per PubSubHandle instance.

```java
ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys);
 
AsyncFunction<PubSubHandle, Long> publishFunction =
        new AsyncFunction<PubSubHandle, Long>() {
            public ListenableFuture<Long> apply(PubSubHandle pubsubHandle) {
                return pubsubHandle.publish("test-channel","Test message.");
            }
        };
ListenableFuture<Long> publishFuture = Futures.transformAsync(connectFuture, publishFunction);
 
Futures.addCallback(publishFuture, new FutureCallback<Long>() {
    public void onSuccess(Long publishResponse) {
        Log.e("TEST","publishResponse:", publishResponse);
    }
    public void onFailure(Throwable error) {
        Log.e("TEST","Error:", error);
    }
}, executor);
```

#### [`publishWithAck (channel, message)`](#publishwithack-channel-message)

Use this if you want to publish with a confirmation from the server.  It will return with a UUID receipt.  If you don't need an acknowledgement, use [`publish(channel, message)`]().  You don't need to be subscribed to a channel to publish to it.

```java
ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys);
 
AsyncFunction<PubSubHandle, UUID> publishWithAckFunction =
        new AsyncFunction<PubSubHandle, UUID>() {
            public ListenableFuture<UUID> apply(PubSubHandle pubsubHandle) {
                return pubsubHandle.publishWithAck("test-channel","Test message.");
            }
        };
ListenableFuture<List<String>> publishWithAckFuture = Futures.transformAsync(connectFuture, publishWithAckFunction);
 
Futures.addCallback(publishWithAckFuture, new FutureCallback<UUID>() {
    public void onSuccess(UUID publishWithAckResponse) {
        Log.e("TEST","publishWithAckResponse:", publishWithAckResponse);
    }
    public void onFailure(Throwable error) {
        Log.e("TEST","Error:", error);
    }
}, executor);
```

#### [`close()`](#close)

Close your connection.  The server will respond with the list of channels you were subscribed to before closing.

```java
ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys);
 
AsyncFunction<PubSubHandle, List<String>> closeFunction =
        new AsyncFunction<PubSubHandle, List<String>>() {
            public ListenableFuture<List<String>> apply(PubSubHandle pubsubHandle) {
                return pubsubHandle.close();
            }
        };
ListenableFuture<List<String>> closeFuture = Futures.transformAsync(connectFuture, closeFunction);
 
Futures.addCallback(closeFuture, new FutureCallback<List<String>>() {
    public void onSuccess(List<String> closeResponse) {
        Log.e("TEST","closeResponse:", closeResponse);
    }
    public void onFailure(Throwable error) {
        Log.e("TEST","Error:", error);
    }
});
```

## [A complete example](#a-complete-example)

In this example, we connect, subscribe, publish, then receive the message in `messageHandler`.

```java
    public void subscribeThenPublishWithoutAck() {

        // Get these keys from your Cogswell.io pub/sub account.
        List<String> keys = new ArrayList<String>();
        keys.add("A-*-*");
        keys.add("R-*-*");
        keys.add("W-*-*");

        // You may want to use an executor that is shared with your app.
        Executor executor = MoreExecutors.directExecutor();
        final CountDownLatch subscribeMessageSignal = new CountDownLatch(1);

        // Track shared state.
        final Map<String,Object> responses = new HashMap<String, Object>();

        final String testChannel = "TEST-CHANNEL";
        final String testMessage = "TEST-MESSAGE:"+System.currentTimeMillis()+"-"+Math.random();
        final PubSubMessageHandler messageHandler = new PubSubMessageHandler() {
            @Override
            public void onMessage(PubSubMessageRecord record) {
                responses.put("subscribeReceivedMessage", record);
                subscribeMessageSignal.countDown();
            }
        };

        ListenableFuture<PubSubHandle> connectFuture = PubSubSDK.getInstance().connect(keys);

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
                signal.countDown();
            }
        }, executor);

        try {
            signal.await(asyncTimeoutSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Log.d("TEST","Server responses:", responses);
    }
```

## [Running the tests](#running-the-tests)
Update `src/androidTest/resources/io/cogswell/sdk/pubsub/config.json` with keys you get from [Cogswell.io](https://cogswell.io).  You can then run the tests in `src/androidTest/java/io/cogswell/sdk/pubsub` from inside Android Studio.