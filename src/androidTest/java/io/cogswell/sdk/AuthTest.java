package io.cogswell.sdk;

//import org.junit.*;
import junit.framework.TestCase;

import org.json.JSONObject;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import io.cogswell.sdk.pubsub.PubSubHandleTest;

public class AuthTest extends TestCase {

    public void testSocketAuth() throws Exception {
        // Load a set of keys.
        InputStream jsonConfigIS = PubSubHandleTest.class.getResourceAsStream("config.json");
        String configJsonString = new Scanner(jsonConfigIS, "UTF-8").useDelimiter("\\A").next();
        List<String> keys = new ArrayList<>();
        JSONObject configJson = new JSONObject(configJsonString);

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

        Auth.socketAuth(keys, null);
    }

}