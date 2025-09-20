package com.senzing.sdk.core.perpetual;

import java.time.Duration;

import com.senzing.sdk.SzException;
import com.senzing.util.LoggingUtilities;

/**
 * Background thread to refresh the configuration on the 
 * {@link SzPerpetualCoreEnvironment}.
 */
class Reinitializer extends Thread {
    /**
     * Constant for converting between nanoseconds and milliseconds.
     */
    private static final long ONE_MILLION = 1000000L;

    /**
     * Constant for converting between seconds and milliseconds.
     */
    private static final long ONE_THOUSAND = 1000L;

    /**
     * The maximum number of errors before giving up on monitoring the active
     * configuration for changes.
     */
    private static final int MAX_ERROR_COUNT = 5;

    /**
     * The {@link com.senzing.sdk.SzEnvironment} to monitor 
     * and reinitialize.
     */
    private SzPerpetualCoreEnvironment env;
    
    /**
     * Flag indicating if the thread should complete or continue monitoring.
     */
    private boolean complete;

    /**
     * Constructs with the {@link SzEnvironment}.
     *
     * @param env The {@link SzEnvironment} to use.
     */
    Reinitializer(SzPerpetualCoreEnvironment env) {
        this.env        = env;
        this.complete   = false;
    }

    /**
     * Signals that this thread should complete execution.
     */
    synchronized void complete() {
        if (this.complete) {
            return;
        }
        this.complete = true;
        this.notifyAll();
    }

    /**
     * Checks if this thread has received the completion signal.
     *
     * @return <tt>true</tt> if the completion signal has been received, 
     *         otherwise <tt>false</tt>.
     */
    synchronized boolean isComplete() {
        return this.complete;
    }

    /**
     * The run method implemented to periodically check if the active
     * configuration ID differs from the default configuration ID 
     * and if so, reinitializes.
     */
    public void run() {
        try {
            int errorCount = 0;
            // loop until completed
            while (!this.isComplete()) {
                // check if we have reached the maximum error count
                if (errorCount > MAX_ERROR_COUNT) {
                    System.err.println(
                        "Giving up on monitoring active configuration after "
                        + errorCount + " failures");
                    return;
                }

                // get the refresh period
                Duration duration = this.env.getConfigRefreshPeriod();

                // check if zero or null (we should not really get here since 
                // this thread should not be started if the delay is zero)
                if (duration == null || duration.isZero()) {
                    this.complete();
                    continue;
                }

                // convert to milliseconds
                long delay = duration.getSeconds() * ONE_THOUSAND 
                    + (duration.getNano() / ONE_MILLION);

                try {
                    // sleep for the delay period
                    Thread.sleep(delay);
                    
                    // check if destroyed
                    if (this.env.isDestroyed()) {
                        this.complete();
                        continue;
                    }

                    // ensure the config is current
                    this.env.ensureConfigCurrent();

                } catch (InterruptedException | SzException e) {
                    errorCount++;
                    continue;
                }

                // reset the error count if we successfully reach this point
                errorCount = 0;
            }

        } catch (Exception e) {
            System.err.println(
                "Giving up on monitoring active configuration due to exception:");
            System.err.println(e.getMessage());
            System.err.println(LoggingUtilities.formatStackTrace(e.getStackTrace()));

        } finally {
            this.complete();
        }
    }

}
