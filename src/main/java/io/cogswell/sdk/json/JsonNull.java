package io.cogswell.sdk.json;

import org.json.JSONObject;

/**
 * Created by jedwards on 5/3/16.
 */
public class JsonNull extends JsonNode {
    protected static final JsonNull singleton = new JsonNull();

    @Override public JsonNode obj(int index) { return singleton; }
    @Override public JsonNode obj(String key) { return singleton; }

    @Override public JsonNode arr(int index) { return singleton; }
    @Override public JsonNode arr(String key) { return singleton; }

    @Override public String str(int index) { return null; }
    @Override public String str(String key) { return null; }

    @Override public Number num(int index) { return null; }
    @Override public Number num(String key) { return null; }

    @Override public Boolean bool(int index) { return null; }
    @Override public Boolean bool(String key) { return null; }

    @Override public boolean isNull() { return true; }
    @Override public boolean isNull(String key) { return true; }
    @Override public boolean isNull(int index) { return true; }

    @Override public boolean has(String key) { return false; }
    @Override public boolean has(int index) { return false; }

    @Override public JSONObject asObject() { return null; }
}
