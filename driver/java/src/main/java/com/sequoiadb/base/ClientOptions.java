package com.sequoiadb.base;

public class ClientOptions {
    private boolean enableCache;
    private long cacheInterval;

    public ClientOptions() {
        enableCache = true;
        cacheInterval = 300 * 1000;
    }

    /**
     * @return The value of "enableCache".
     * @fn boolean getEnableCache()
     * @brief Get the value of "enableCache".
     */
    public boolean getEnableCache() {
        return enableCache;
    }

    /**
     * @param enable true or false.
     * @return void
     * @fn void setEnableCache(boolean enable)
     * @brief Set caching the name of collection space and collection in client or not.
     */
    public void setEnableCache(boolean enable) {
        enableCache = enable;
    }

    /**
     * @return The value of caching interval.
     * @fn long getCacheInterval()
     * @brief Get the caching interval.
     */
    public long getCacheInterval() {
        return cacheInterval;
    }

    /**
     * @param interval The interval in milliseconds.
     * @return void
     * @fn void setCacheInterval(long interval)
     * @brief Set the interval for caching the name of collection space
     * and collection in client in milliseconds.
     * This value should not be less than 0,
     * or it will be set to the default value,
     * default to be 300*1000ms.
     */
    public void setCacheInterval(long interval) {
        cacheInterval = interval;
    }

}
