package io.cogswell.sdk.json;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by jedwards on 5/3/16.
 */
public abstract class JsonNode {
    public abstract JsonNode obj(int index);
    public abstract JsonNode obj(String key);

    public abstract JsonNode arr(int index);
    public abstract JsonNode arr(String key);

    public abstract String str(int index);
    public abstract String str(String key);

    public abstract Number num(int index);
    public abstract Number num(String key);

    public abstract Boolean bool(int index);
    public abstract Boolean bool(String key);

    public abstract boolean isNull();
    public abstract boolean isNull(String key);
    public abstract boolean isNull(int index);

    public abstract boolean has(String key);
    public abstract boolean has(int index);

    public abstract JSONObject asObject();
}
