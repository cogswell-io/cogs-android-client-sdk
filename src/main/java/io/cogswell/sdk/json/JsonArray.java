package io.cogswell.sdk.json;

import android.support.v7.widget.LinearLayoutCompat;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by jedwards on 5/3/16.
 */
public class JsonArray extends JsonNode {
    private static JsonNull NULL = JsonNull.singleton;

    private JSONArray array;

    public JsonArray(JSONArray array) {
        this.array = array;
    }

    @Override public JsonNode obj(String key) { return NULL; }
    @Override public JsonNode obj(int index) {
        if (array == null)
            return NULL;

        try {
            if (array.opt(index) == null)
                return NULL;

            JSONObject obj = array.getJSONObject(index);

            return (obj == null) ? NULL : new JsonObject(obj);
        } catch (JSONException e) {
            return NULL;
        }
    }

    @Override public JsonNode arr(String key) { return NULL; }
    @Override public JsonNode arr(int index) {
        if (array == null)
            return NULL;

        try {
            if (array.opt(index) == null)
                return NULL;

            JSONArray arr = array.getJSONArray(index);

            return (arr == null) ? NULL : new JsonArray(arr);
        } catch (JSONException e) {
            return NULL;
        }
    }

    @Override public String str(String key) { return null; }
    @Override public String str(int index) {
        if (array == null)
            return null;

        try {
            return array.getString(index);
        } catch (JSONException e) {
            return null;
        }
    }

    @Override public Number num(String key) { return null; }
    @Override public Number num(int index) {
        if (array == null)
            return null;

        try {
            return array.getDouble(index);
        } catch (JSONException e) {
            return null;
        }
    }

    @Override public Boolean bool(String key) { return null; }
    @Override public Boolean bool(int index) {
        if (array == null)
            return null;

        try {
            return array.getBoolean(index);
        } catch (JSONException e) {
            return null;
        }
    }

    @Override public boolean isNull() { return false; }
    @Override public boolean isNull(String key) { return true; }
    @Override public boolean isNull(int index) { return array == null ? true : array.isNull(index); }

    @Override public boolean has(String key) { return false; }
    @Override public boolean has(int index) { return array == null ? false : array.length() < index; }

    @Override public JSONObject asObject() { return null; }
}
