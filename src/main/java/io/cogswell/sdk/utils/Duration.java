package io.cogswell.sdk.utils;

import java.util.concurrent.TimeUnit;

/**
 * Represents some length of time, and provides a few
 * simple utility methods.
 *
 * @author Joel Edwards &lt;jedwards@aviatainc.com&gt;
 * @since 2017-02-20
 */
public class Duration {
    private long length;
    private TimeUnit unit;

    public Duration(long length, TimeUnit unit) {
        this.length = length;
        this.unit = unit;
    }

    /**
     * Convert to a new Duration converted to the specified {@link TimeUnit}.
     *
     * @param newUnit the {@link TimeUnit} for the new {@link Duration}
     *
     * @return the new, adjusted {@link Duration}
     */
    public Duration to(TimeUnit newUnit) {
        return of(newUnit.convert(length, unit), newUnit);
    }

    /**
     * Returns the length translated to the desired {@link TimeUnit}.
     *
     * @param newUnit the {@link TimeUnit} for the translated length
     *
     * @return the length in terms of the specified {@link TimeUnit}
     */
    public long as(TimeUnit newUnit) {
        return newUnit.convert(length, unit);
    }

    public long getLength() {
        return length;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    /**
     * Returns a new {@link Duration} denoting the specified length of time.
     *
     * @param length the length of time
     * @param unit the {@link TimeUnit unit} of time for <tt>length</tt>
     *
     * @return the new {@link Duration}
     */
    public static Duration of(long length, TimeUnit unit) {
        return new Duration(length, unit);
    }
}
