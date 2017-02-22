package io.cogswell.sdk.utils;

/**
 * "A useful pot to put things in." -- Winnie the Pooh
 *
 * Helpful when you need to store something in a closed reference.
 *
 * @author Joel Edwards &lt;jedwards@aviatainc.com&gt;
 * @since 2017-02-21
 */
public class Container<T> {
    private T value;

    /**
     * Set the value of this container, returning it to allow for chaining.
     *
     * @param value the value to store
     *
     * @return the value which was stored
     */
    public T set(T value) {
        this.value = value;
        return value;
    }

    /**
     * Supplies the value stored in this container, if any
     *
     * @return the value
     */
    public T get() {
        return value;
    }
}
