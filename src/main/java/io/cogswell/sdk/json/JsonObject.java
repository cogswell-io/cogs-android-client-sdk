package io.cogswell.sdk.json;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by jedwards on 5/3/16.
 */
public class JsonObject extends JsonNode {
    private static JsonNull NULL = JsonNull.singleton;

    private JSONObject object;

    public JsonObject(JSONObject object) {
       this.object = object;
    }

    @Override public JsonNode obj(int index) { return NULL; }
    @Override public JsonNode obj(String key) {
        try {
            if (object == null)
                return NULL;

            if (!object.has(key))
                return NULL;

            JSONObject obj = object.getJSONObject(key);

            return (obj == null) ? NULL : new JsonObject(obj);
        } catch (JSONException e) {
            return NULL;
        }
    }

    @Override public JsonNode arr(int index) { return NULL; }
    @Override public JsonNode arr(String key) {
        if (object == null)
            return NULL;

        try {
            if (!object.has(key))
                return NULL;

            JSONArray arr = object.getJSONArray(key);

            return (arr == null) ? NULL : new JsonArray(arr);
        } catch (JSONException e) {
            return NULL;
        }
    }

    @Override public String str(int index) { return null; }
    @Override public String str(String key) {
        if (object == null)
            return null;

        try {
            return object.getString(key);
        } catch (JSONException e) {
            return null;
        }
    }

    @Override public Number num(int index) { return null; }
    @Override public Number num(String key) {
        if (object == null)
            return null;

        try {
            return object.getDouble(key);
        } catch (JSONException e) {
            return null;
        }
    }

    @Override public Boolean bool(int index) { return null; }
    @Override public Boolean bool(String key) {
        if (object == null)
            return null;

        try {
            return object.getBoolean(key);
        } catch (JSONException e) {
            return null;
        }
    }

    @Override public boolean isNull() { return false; }
    @Override public boolean isNull(String key) { return object == null ? true : object.isNull(key); }
    @Override public boolean isNull(int index) { return true; }

    @Override public boolean has(String key) { return object == null ? false : object.has(key); }
    @Override public boolean has(int index) { return false; }

    @Override public JSONObject asObject() { return object; }
}
