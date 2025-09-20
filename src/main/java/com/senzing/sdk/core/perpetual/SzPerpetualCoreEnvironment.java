package com.senzing.sdk.core.perpetual;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzConfigRetryable;
import com.senzing.sdk.SzDiagnostic;
import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzException;
import com.senzing.sdk.SzProduct;
import com.senzing.sdk.SzRetryableException;
import com.senzing.sdk.core.SzCoreEnvironment;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * Extends {@link SzCoreEnvironment} to provide a more robust 
 * implementation for instances used in long-running processes
 * typically in server-side applications.  The features added
 * by this implementation are:
 * <ul>
 *  <li>
 *      Optional basic retry logic to retry <b>any</b> Senzing
 *      Core SDK method that fails with an {@link 
 *      SzRetryableException}.  The method may be retried one
 *      or more times with an increasing delay between retry
 *      attempts.
 *  </li>
 *  <li>
 *      Optional automatic configuration refresh so that the
 *      active configuration ID remains in sync with the 
 *      current default configuration ID.
 *  </li>
 *  <li>
 *      When automatic configuration refresh is enabled, this
 *      implementation automatically refreshes the configuration
 *      when a Senzing Core SDK method annotated as
 *      {@link SzConfigRetryable} fails with an {@link 
 *      SzException}, subsequently retrying that method if in
 *      fact the the active configuration was changed.
 *  </li>
 *  <li>
 *      Optional isolation of all Senzing Core SDK operations
 *      to an execution thread pool to enhance performance by
 *      reusing cached thread-local resources while preventing
 *      excessive memory usage that could occur when executing
 *      Senzing operations in too many threads.
 *  </li>
 *  <li>
 *      A limited {@link ExecutorService}-like interface to 
 *      enable executing multiple Senzing Core SDK operations
 *      within the execution thread pool while preventing
 *      excessive context switching.
 *  </li>
 * </ul>
 */
public class SzPerpetualCoreEnvironment extends SzCoreEnvironment {
    /**
     * The thread-local retry flag.
     */
    static final ThreadLocal<Boolean> RETRY_FLAG = new ThreadLocal<>() {
        protected Boolean initialValue() {
            return Boolean.FALSE;
        }
    };

    /**
     * The thread-local flag indicating if a method was retried.
     */
    private static final ThreadLocal<Boolean> RETRIED_FLAG = new ThreadLocal<>() {
        protected Boolean initialValue() {
            return Boolean.FALSE;
        }
    };

    /**
     * The number of milliseconds to delay (if not notified) until checking
     * if we are destroyed.
     */
    private static final long DESTROY_DELAY = 5000L;

    /**
     * The default number of times this class will perform a basic 
     * retry when an {@link SzRetryableException} occurs.  The value
     * of this constant is {@value}.
     */
    public static final int DEFAULT_MAX_BASIC_RETRIES = 2;

    /**
     * The classes to be proxied using the {@link RetryHandler}.
     */
    private static final Set<Class<?>> PROXY_CLASSES = Set.of(SzEngine.class,
                                                               SzProduct.class,
                                                               SzConfigManager.class,
                                                               SzDiagnostic.class,
                                                               SzConfig.class);

    /**
     * The maximum number of time to try to reinitialize with the
     * latest default configuration ID before giving up.  This ensures
     * we never get an infinite loop in the case of race conditions.
     */
    private static final int MAX_REINITIALIZE_COUNT = 5;

    /**
     * The number of seconds to wait for shutdown of the internal
     * {@link ExecutorService} and the internal {@link Reinitializer}.
     */
    private static final long SHUTDOWN_WAIT_SECONDS = 5;

    /**
     * The maximum number of times to wait for shutdown of the internal
     * {@link ExecutorService} and the internal {@link Reinitializer}.
     */
    private static final int SHUTDOWN_WAIT_COUNT = 3;

    /**
     * A <code>null</code> {@link Duration} reference that can be
     * used with {@link Builder#configRefreshPeriod(Duration)} to
     * disable configuration refresh and automatic retry of failed
     * methods that are annotated with {@link SzConfigRetryable}.
     */
    public static final Duration DISABLED_CONFIG_REFRESH = null;

    /**
     * A zero (0) {@link Duration} instance that can be used 
     * with {@link Builder#configRefreshPeriod(Duration)} to
     * enable <b>Reactive</b> configuration refresh so that
     * configuration refresh and automatic retry are performed
     * on demand when a method annotated with {@link 
     * SzConfigRetryable} fails with an {@link SzException}.
     */
    public static final Duration REACTIVE_CONFIG_REFRESH
        = Duration.ofSeconds(0);
    
    /**
     * A <code>null</code> {@link Integer} reference that can be 
     * used with {@link Builder#concurrency(Integer)} to disable
     * the isolated concurrency thread pool and enable execution
     * in the calling thread.
     */
    public static final Integer DISABLED_CONCURRENCY = null;

    /**
     * A zero (0) {@link Integer} instance that can be used with
     * {@link Builder#concurrency(Integer)} to indicate that the
     * concurrency should be set to {@link 
     * Runtime#availableProcessors()}.
     */
    public static final Integer RECOMMENDED_CONCURRENCY = 0;

    /**
     * Enumerates the various modes for configuration refresh.
     * 
     * <p>
     * Configuration refresh is the process by which the 
     * {@linkplain com.senzing.sdk.SzEnvironment#getActiveConfigId()
     * active configuration ID} is compared to the current 
     * {@linkplain com.senzing.sdk.SzConfigManager#getDefaultConfigId()
     * default configuration ID} and if they are out of sync, the 
     * {@link SzPerpetualCoreEnvironment} is {@linkplain 
     * com.senzing.sdk.SzEnvironment#reinitialize(long) reinitialized}
     * with the current default configuration ID.
     * </p>
     */
    public enum RefreshMode {
        /**
         * The configuration will never be automatically refreshed.  
         * Methods annotated with {@link SzConfigRetryable} will 
         * <b>not</b> be retried upon an exception.
         */
        DISABLED,

        /**
         * The configuration will be refreshed on-demand in response
         * to an exception being thrown by a method annotated with
         * {@link SzConfigRetryable}.  When this occurs, the method will
         * be retried to see if it still fails after refresh.
         */
        REACTIVE,

        /**
         * The configuration will be refreshed in the background
         * periodically using a specified {@link Duration} as the 
         * refresh period.  This mode <b>ALSO</b> performs the 
         * on-demand refresh as with <b>Reactive</b> mode.
         */
        PROACTIVE;
    }

    /**
     * Provides an interface for initializing an instance of
     * {@link SzPerpetualCoreEnvironment}.
     * 
     * <p>
     * This interface is not needed to use {@link SzPerpetualCoreEnvironment}.
     * It is only needed if you are extending {@link SzPerpetualCoreEnvironment}.
     * </p>
     * 
     * <p>
     * This is provided for derived classes of {@link SzPerpetualCoreEnvironment}
     * to initialize their super class and is typically implemented by
     * extending {@link AbstractBuilder} in creating a derived
     * builder implementation.
     * </p>
     */
    public interface Initializer extends SzCoreEnvironment.Initializer {
        /**
         * Gets the configuration refresh period for this instance.
         * 
         * <p>
         * Configuration refresh is the process by which the 
         * {@linkplain com.senzing.sdk.SzEnvironment#getActiveConfigId()
         * active configuration ID} is compared to the current 
         * {@linkplain com.senzing.sdk.SzConfigManager#getDefaultConfigId()
         * default configuration ID} and if they are out of sync, the 
         * {@link SzPerpetualCoreEnvironment} is {@linkplain 
         * com.senzing.sdk.SzEnvironment#reinitialize(long) reinitialized}
         * with the current default configuration ID.
         * </p>
         * 
         * <p>
         * Configuration refresh can be configured in one of three modes
         * enumerated by {@link RefreshMode}.
         * </p>
         * 
         * <p>
         * <b>NOTE:</b> Configuration refresh cannot be in any mode
         * other than {@link RefreshMode#DISABLED} if the {@linkplain 
         * #getConfigId() configuration ID} is a non-null value.  An
         * exception will be thrown if attempting to construct an {@link 
         * SzPerpetualCoreEnvironment} with an explicit configuration ID
         * with configuration refresh enabled.
         * </p>
         * 
         * <p>
         * The possible values for this setting with their associated
         * mode are as follows:
         * <ul>
         *  <li>
         *      A <code>null</code> value implies that configuration 
         *      refresh is {@linkplain RefreshMode#DISABLED disabled}
         *      (see {@link #DISABLED_CONFIG_REFRESH}).
         *  <li>
         *  <li>
         *      A zero (0) {@link Duration} implies that configuration 
         *      refresh will be {@linkplain RefreshMode#REACTIVE reactive}
         *      (see {@link #REACTIVE_CONFIG_REFRESH}).
         *  </li>
         *  <li>
         *      A positive {@link Duration} implies that configuration
         *      refresh will be {@linkplain RefreshMode#PROACTIVE proactive}
         *      with the {@link Duration} value used as the refresh period.
         *  </li>
         * </ul>
         * 
         * @return A positive {@link Duration} to enable {@linkplain
         *         RefreshMode#PROACTIVE proactive} configuration refresh,
         *         a zero (0) {@link Duration} to enable {@linkplain
         *         RefreshMode#REACTIVE reactive} configuration refresh or 
         *         <code>null</code> to {@linkplain RefreshMode#DISABLED
         *         disable} configuration refresh.
         * 
         * @see #DISABLED_CONFIG_REFRESH
         * @see #REACTIVE_CONFIG_REFRESH
         */
        Duration getConfigRefreshPeriod();

        /**
         * Gets the concurrency for the thread pool used to execute the
         * Senzing Core SDK operations.
         * 
         * <p>
         * The Senzing Core SDK allocates resources that are local to each
         * thread.  These resources are allocated when first needed and then
         * cached per thread to enhance performance of subsequent Core SDK
         * operations performed on that thread.  As such, it is best to reuse
         * the same threads each time for the Core SDK operations <b>both</b>
         * for performance reasons and to prevent excessive memory allocation.
         * </p>
         * 
         * <p>
         * Server applications that leverage the Senzing Core SDK may have 
         * a large number of threads for servicing requests, but should limit
         * the concurrency for the Senzing Core SDK to at most the number of 
         * processing cores on the machine (or less if memory is limited).
         * </p>
         * 
         * The provided value determines the behavior:
         * <ul>
         *  <li>
         *      If <code>null</code> (see {@link #DISABLED_CONCURRENCY})
         *      then the thread pool is disabled and all operations are
         *      performed in the calling thread.  This may be desirable
         *      if you have an {@link ExecutorService} that you are 
         *      already using to perform one more Senzing Core SDK 
         *      operations in conjunction and you want to avoid 
         *      unnecessary context switching.
         *  </li>
         *  <li>
         *      If zero (0) (see {@link #RECOMMENDED_CONCURRENCY}) then
         *      the thread pool will be enabled with a number of threads
         *      equal to the number of processing cores as determined by
         *      {@link Runtime#availableProcessors()}.
         *  </li>
         *  <li>
         *      If a positive integer then the thread pool will be enabled
         *      using that number of threads.
         *  </li>
         * </ul>
         * 
         * <p>
         * Any created thread pool will be disposed upon calling {@link
         * #destroy()}.
         * </p>
         * 
         * @return A positive integer indicating the size of the thread pool, 
         *         zero (0) to use the number of threads equal to {@link
         *         Runtime#availableProcessors()}, or <code>null</code> to
         *         disable the thread pool.
         * 
         * @see #DISABLED_CONCURRENCY
         * @see #RECOMMENDED_CONCURRENCY
         * @see #submitTask(Callable)
         * @see #submitTask(Runnable)
         * @see #submitTask(Runnable,Object)
         */
        Integer getConcurrency();

        /**
         * Returns the maximum number of times a method invocation should be 
         * retried either due to an {@link SzRetryableException} being 
         * encountered.
         * 
         * <p>
         * There are several things to note:
         * </p>
         * <ol>
         *      <li>This setting has <b>no</b> effect on the retrying of 
         *          Senzing Core SDK methods annotated with {@link
         *          SzConfigRetryable} methods as that is simply governed by
         *          the active configuration requiring a refresh.
         *      </li>
         *      <li>Each subsequent retry for will occur after increasingly 
         *          long delay to allow the condition causing the failure to
         *          be resolved.
         *      </li>
         *      <li>Since the basic retries are nested with the configuration 
         *          refresh retries, it is possible, though unlikely, for the
         *          total number of retries to exceed the maximum.  For example,
         *          a method may fail due to a missing data source in the active 
         *          configuration, triggering a retry after the configuration
         *          refresh, and the retried attempt may then fail due to a
         *          persistent deadlock condition causing multiple {@link 
         *          SzRetryableException}'s being thrown.
         *      </li>
         * </ol>
         * 
         * @return The maximum number of times to retry a method invocation 
         *         due to an {@link SzRetryableException}.
         */
        int getMaxBasicRetries();
    }

    /**
     * Extends {@link SzCoreEnvironment.AbstractBuilder} to provide
     * additional initialization properties for {@link 
     * SzPerpetualCoreEnvironment}.
     * 
     * @param <E> The {@link SzPerpetualCoreEnvironment}-derived class
     *            built by instances of this class.
     * @param <B> The {@link AbstractBuilder}-derived class of the
     *            implementation.
     */
    public abstract static 
    class AbstractBuilder<E extends SzPerpetualCoreEnvironment, 
                          B extends AbstractBuilder<E, B>>
        extends SzCoreEnvironment.AbstractBuilder<E, B> 
        implements Initializer
    {
        /**
         * The number of threads for executing, or 
         * <code>null</code> if the thread pool is disabled.
         */
        private Integer concurrency = null;

        /**
         * The period with which to background-refresh the 
         * active configuration, or <code>null</code> if 
         * configuration refresh is disabled.
         */
        private Duration configRefreshPeriod = null;

        /**
         * The maximum number of times to perform a retry of a
         * method due to an {@link SzRetryableException}.
         */
        private int maxBasicRetries = DEFAULT_MAX_BASIC_RETRIES;

        /**
         * Default constructor.
         */
        protected AbstractBuilder() {
            super();
            this.concurrency            = DISABLED_CONCURRENCY;
            this.configRefreshPeriod    = DISABLED_CONFIG_REFRESH;
            this.maxBasicRetries        = DEFAULT_MAX_BASIC_RETRIES;
        }

        /**
         * Sets the configuration refresh period for this instance.
         * If not explicitly called, the default value will be 
         * {@link #DISABLED_CONFIG_REFRESH}.
         * 
         * <p>
         * See {@link #getConfigRefreshPeriod()} for a description
         * of how the specified duration relates to the configuration
         * refresh modes.
         * </p>
         * 
         * @param duration A positive {@link Duration} to enable <b>Proactive</b>
         *                 configuration refresh, a zero (0) {@link Duration} to
         *                 enable <b>Reactive</b> configuration refresh or 
         *                 <code>null</code> to <b>Disable</b> configuration
         *                 refresh.
         * 
         * @return A reference to this instance.
         * 
         * @throws IllegalArgumentException If a negative {@link Duration} is 
         *                                  specified.
         * 
         * @see #DISABLED_CONFIG_REFRESH
         * @see #REACTIVE_CONFIG_REFRESH
         */
        @SuppressWarnings("unchecked")
        public B configRefreshPeriod(Duration duration) 
        {
            if (duration != null && duration.isNegative()) {
                throw new IllegalArgumentException(
                    "The specified duration cannot be negative: " 
                    + duration);
            }
            this.configRefreshPeriod = duration;
            return ((B) this);
        }

        /**
         * Implemented to return the configuration refresh period,
         * or <code>null</code> if none has been configured.
         * 
         * {@inheritDoc}
         */
        @Override
        public Duration getConfigRefreshPeriod() {
            return this.configRefreshPeriod;
        }

        /**
         * Sets the concurrency for the thread pool used to execute the
         * Senzing Core SDK operations.  If not explicitly called, the
         * default value will be {@link #DISABLED_CONCURRENCY}.
         * 
         * <p>
         * See {@link #getConcurrency()} for a description of how the
         * specified concurrency affects the thread pool and how it is used.
         * </p>
         * 
         * @param concurrency A positive integer indicating the size of
         *                    the thread pool, zero (0) to use the a number
         *                    of threads equal to {@link
         *                    Runtime#availableProcessors()}, or
         *                    <code>null</code> to disable the thread pool.
         * 
         * @return A reference to this instance.
         * 
         * @throws IllegalArgumentException If a negative value is specified.
         * 
         * @see #DISABLED_CONCURRENCY
         * @see #RECOMMENDED_CONCURRENCY
         * @see #submitTask(Callable)
         * @see #submitTask(Runnable)
         * @see #submitTask(Runnable,Object)
         */
        @SuppressWarnings("unchecked")
        public B concurrency(Integer concurrency)
            throws IllegalArgumentException
        {
            if (concurrency != null && concurrency < 0) {
                throw new IllegalArgumentException(
                    "The specified concurrency cannot be negative: "
                        + concurrency);
            }
            this.concurrency = concurrency;
            return ((B) this);
        }

        /**
         * Implemented to return the concurrency or <code>null</code> 
         * if none has been configured.
         * 
         * {@inheritDoc}
         */
        @Override
        public Integer getConcurrency() {
            return this.concurrency;
        }

        /**
         * Sets the maximum number of basic retries to perform when an 
         * {@link SzRetryableException} is encountered when invoking an
         * Senzing Core SDK operation.
         * 
         * <p>
         * See {@link #getMaxBasicRetries()} for a description of how the
         * maximum is applied.
         * </p>
         * 
         * @param maxRetries A non-negative integer indicating the maximum
         *                   number of times to retry a Senzing Core SDK 
         *                   operation that fails with an 
         *                   {@link SzRetryableException}.
         * 
         * @return A reference to this instance.
         * 
         * @throws IllegalArgumentException If a negative value is specified.
         * 
         * @see #DEFAULT_MAX_BASIC_RETRIES
         */
        @SuppressWarnings("unchecked")
        public B maxBasicRetries(int maxRetries)
            throws IllegalArgumentException
        {
            if (maxRetries < 0) {
                throw new IllegalArgumentException(
                    "The specified maximum number of retries cannot be negative: "
                        + maxRetries);
            }
            this.maxBasicRetries = maxRetries;
            return ((B) this);
        }

        /**
         * Implemented to return the configuration refresh period,
         * or <code>null</code> if none has been configured.
         * 
         * {@inheritDoc}
         */
        @Override
        public int getMaxBasicRetries() {
            return this.maxBasicRetries;
        }
    }

    /**
     * The builder class for creating an instance of 
     * {@link SzPerpetualCoreEnvironment}.
     */
    public static class Builder 
        extends AbstractBuilder<SzPerpetualCoreEnvironment, Builder> 
    {
        /**
         * Default constructor.
         */
        public Builder() {
            super();
        }

        /**
         * Creates a new {@link SzPerpetualCoreEnvironment} instance based on
         * this {@link Builder} instance.  This method will throw an {@link 
         * IllegalStateException} if another active {@link SzCoreEnvironment}
         * instance exists since only one active instance can exist within a
         * process at any given time.  An active instance is one that has
         * been constructed, but has not yet been destroyed.
         * 
         * @return The newly created {@link SzPerpetualCoreEnvironment} instance.
         * 
         * @throws IllegalStateException If another active {@link SzCoreEnvironment}
         *                               instance exists when this method is
         *                               invoked or if there is an explicit
         *                               configuration ID and the configuration
         *                               refresh period is <b>not</b> 
         *                               <code>null</code>.
         */
        @Override
        public SzPerpetualCoreEnvironment build() 
            throws IllegalStateException
        {
            if (this.getConfigId() != null 
                && this.getConfigRefreshPeriod() != null)
            {
                throw new IllegalStateException(
                    "Cannot provide an explicit configuration ID (" 
                    + this.getConfigId() + ") and enable configuration "
                    + "refresh with a non-null configuration refresh "
                    + "period (" + this.getConfigRefreshPeriod()
                    + ").");
            }

            return new SzPerpetualCoreEnvironment(this);
        }
    }

    /**
     * Extends {@link Thread} to allow for identifying of core threads.
     */
    static class CoreThread extends Thread {
        /**
         * Constructs with the specified {@link Runnable}.
         * 
         * @param runnable The {@link Runnable} with which to construct with.
         */
        CoreThread(Runnable runnable) {
            super(runnable);
        }
    }

    /**
     * The {@link InvocationHandler} implementation that sets the 
     * thread-local {@link #RETRY_FLAG} if a method is retryable.
     */
    private static class RetryHandler implements InvocationHandler {
        /**
         * The target object for which to handle methods.
         */
        private Object target = null;

        /**
         * Constructs with the target object.
         * 
         * @param target The target object for which to handle methods.
         */
        RetryHandler(Object target) {
            this.target = target;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable 
        {
            SzConfigRetryable retryable 
                = method.getAnnotation(SzConfigRetryable.class);
 
            Object result = null;
            // check if no annotation and just do a standard invocation
            if (retryable == null) {
                try {
                    return method.invoke(target, args);

                } catch (InvocationTargetException e) {
                    // get the cause of the exception
                    throw e.getCause();
                }

            } else {
                Boolean initial = RETRY_FLAG.get();
                RETRY_FLAG.set(Boolean.TRUE);
                try {
                    result = method.invoke(target, args);

                } catch (InvocationTargetException e) {
                    throw e.getCause();

                } finally {
                    // set the flag back to what our caller set
                    RETRY_FLAG.set(initial);
                }
            }

            // check the result to see if we need to proxy it
            final Object r = result;
            if (r != null
                && (PROXY_CLASSES.contains(method.getReturnType())
                    || PROXY_CLASSES.stream().anyMatch((c) -> c.isInstance(r))))
            {
                Class<?>[] interfaces = result.getClass().getInterfaces();
                ClassLoader classLoader = this.getClass().getClassLoader();
                RetryHandler handler = new RetryHandler(result);
                result = Proxy.newProxyInstance(classLoader, interfaces, handler);
            }

            // return the result (possibly proxied)
            return result;
        }
    }

    /**
     * The {@link ThreadFactory} to be used by instances of this class.
     */
    private static final ThreadFactory THREAD_FACTORY = (r) -> new CoreThread(r);

    /**
     * Creates a new instance of {@link Builder} for setting up an instance
     * of {@link SzPerpetualCoreEnvironment}.  Keep in mind that while multiple 
     * {@link Builder} instances can exists, <b>only one active instance</b> of 
     * {@link SzCoreEnvironment} (including any of its derived classes) can exist
     * at time.  An active instance is one that has not yet been destroyed.
     * 
     * <p>
     * <b>NOTE:</b> The static method {@link #newBuilder()} will produce an
     * instance of {@link SzCoreEnvironment.Builder} which will only create
     * instances of {@link SzCoreEnvironment} rather than {@link 
     * SzPerpetualCoreEnvironment}.
     * </p>
     * 
     * <p>
     * <b>Alternatively</b>, you can directly call the {@link Builder#Builder()}
     * constructor.
     * </p>
     * 
     * @return The {@link Builder} for configuring and initializing the
     *         {@link SzPerpetualCoreEnvironment}.
     */
    public static Builder newPerpetualBuilder() {
        return new Builder();
    }

    /**
     * The {@link ExecutorService} to be used by this instance, or 
     * <code>null</code> if the thread pool is disabled.
     */
    private ExecutorService coreExecutor = null;

    /**
     * The concurrency for this instance.
     */
    private int concurrency;

    /**
     * The configuration refresh period.
     */
    private Duration configRefreshPeriod;

    /**
     * The configuration {@link RefreshMode} for this instance.
     */
    private RefreshMode refreshMode = null;

    /**
     * The maximum number of times each method invocation should
     * be retried when an {@link SzRetryableException} is encountered.
     */
    private int maxBasicRetries = DEFAULT_MAX_BASIC_RETRIES;

    /**
     * Flag indicating if this instance has had its 
     * {@link #destroy()} method called.
     */
    private boolean destroying = false;

    /**
     * The {@link Reinitializer} thread to background refresh
     * the configuration.
     */
    private Reinitializer reinitializer = null;

    /**
     * The {@link ReadWriteLock} for this instance.
     */
    private final ReadWriteLock readWriteLock;

    /**
     * The proxied {@link SzEngine} for this instance.
     */
    private SzEngine engine = null;

    /**
     * The proxied {@link SzConfigManager} for this instance.
     */
    private SzConfigManager configManager = null;

    /**
     * The proxied {@link SzDiagnostic} for this instance.
     */
    private SzDiagnostic diagnostic = null;

    /**
     * The proxied {@link SzProduct} for this instance.
     */
    private SzProduct product = null;

    /**
     * The number of times the configuration was refreshed.
     */
    private int configRefreshCount = 0;

    /**
     * The total number of method calls that were retried.
     */
    private int retriedCount = 0;

    /**
     * The total number of method calls that were retried and failed.
     */
    private int retriedFailureCount = 0;

    /**
     * Internal monitor for synchronization.
     */
    private final Object monitor = new Object();

    /**
     * Protected constructor used by the {@link Builder} to construct the
     * instance.
     *  
     * @param initializer The {@link Initializer} with which to construct
     *                    (typically an instance of {@link AbstractBuilder}).
     */
    protected SzPerpetualCoreEnvironment(Initializer initializer) 
    {
        super(initializer);

        this.readWriteLock  = new ReentrantReadWriteLock(true);

        // determine the number of threads we need in the thread pool
        Integer threadCount = initializer.getConcurrency();
        if (threadCount == null) {
            threadCount = 0;
        } else if (threadCount == 0) {
            threadCount = Runtime.getRuntime().availableProcessors();

        } else if (threadCount < 0) {
            throw new IllegalArgumentException(
                "The concurrency cannot be negative: " + threadCount);
        }
        this.concurrency = threadCount;

        // determine the maximum number of basic retries
        this.maxBasicRetries = initializer.getMaxBasicRetries();

        if (this.maxBasicRetries < 0) {
            throw new IllegalArgumentException(
                "The maximum number of basic retries cannot be negative: "
                + this.maxBasicRetries);
        }

        // determine the configuration refresh period and mode
        this.configRefreshPeriod = initializer.getConfigRefreshPeriod();
        
        if (this.configRefreshPeriod == null) {
            this.refreshMode = RefreshMode.DISABLED;
        } else if (this.configRefreshPeriod.isZero()) {
            this.refreshMode = RefreshMode.REACTIVE;
        } else if (this.configRefreshPeriod.isNegative()) {
            throw new IllegalArgumentException(
                "The configuration refresh period cannot be negative: "
                + this.configRefreshPeriod);
        } else {
            this.refreshMode = RefreshMode.PROACTIVE;
        }

        // setup the executor service
        this.coreExecutor = (threadCount == 0) ? null
            : Executors.newFixedThreadPool(threadCount, THREAD_FACTORY);

        // setup the background configuration refresh if needed
        if (this.refreshMode == RefreshMode.PROACTIVE) {
            this.reinitializer = new Reinitializer(this);
            this.reinitializer.start();
        }
    }

    /**
     * Gets the concurrency with which this instance was initialized.
     * 
     * <p>
     * The value returned will be zero (0) if the thread pool has been
     * disabled, otherwise it will be the number of threads in the 
     * pool.
     * </p>
     * 
     * @return The number of threads in the thread pool for the internal
     *         {@link ExecutorService}, or zero (0) if threading is disabled.
     */
    public int getConcurrency() {
        Lock lock = null;
        try {
            lock = this.acquireReadLock();
            this.ensureNotDestroyed();
            return this.concurrency;
        } finally {
            lock = releaseLock(lock);
        }
    }

    /**
     * Gets the maximum number of basic retries that will be 
     * attempted when a Senzing Core SDK operation fails with an
     * {@link SzRetryableException}.
     * 
     * <p>
     * See {@link Initializer#getMaxBasicRetries()} for a
     * description of how the maximum is applied.
     * </p>
     * 
     * @return The maximum number of basic retries that will be 
     *         attempted when a Senzing Core SDK operation fails 
     *         with an {@link SzRetryableException}.
     */
    public int getMaxBasicRetries() {
        Lock lock = null;
        try {
            lock = this.acquireReadLock();
            this.ensureNotDestroyed();
            return this.maxBasicRetries;
        } finally {
            lock = releaseLock(lock);
        }
    }

    /**
     * Gets the total number of times the configuration was automatically
     * refreshed either periodically or due to an exception on a method
     * annotated with {@link SzConfigRetryable}.
     *  
     * <p>
     * <b>NOTE:</b> This does <b>NOT</b> include explicit calls to 
     * {@link #reinitialize(long)}.
     * </p>
     * 
     * @return The total number of times the configuration was automatically
     *         refreshed
     */
    public int getConfigRefreshCount() {
        Lock lock = null;
        try {
            lock = this.acquireReadLock();
            this.ensureNotDestroyed();
            synchronized (this.monitor) {
                return this.configRefreshCount;
            }
        } finally {
            lock = releaseLock(lock);
        }
    }

    /**
     * Gets the total number Senzing Core SDK method invocations 
     * that initially failed and were retried whether or not they
     * ultimately succeeded.
     *  
     * @return The total number Senzing Core SDK method invocations
     *         that initially failed and were retried.
     */
    public int getRetriedCount() {
        Lock lock = null;
        try {
            lock = this.acquireReadLock();
            this.ensureNotDestroyed();
            synchronized (this.monitor) {
                return this.retriedCount;
            }
        } finally {
            lock = releaseLock(lock);
        }
    }

    /**
     * Gets the total number Senzing Core SDK method invocations 
     * that initially failed, were retried at least once and
     * ultimately failed.
     * 
     * @return The total number Senzing Core SDK method invocations
     *         that initially failed, were retried and ultimately failed.
     */
    public int getRetriedFailureCount() {
        Lock lock = null;
        try {
            lock = this.acquireReadLock();
            this.ensureNotDestroyed();
            synchronized (this.monitor) {
                return this.retriedFailureCount;
            }
        } finally {
            lock = releaseLock(lock);
        }
    }

    /**
     * Ensures this instance is still active and if not will throw 
     * an {@link IllegalStateException}.
     *
     * @throws IllegalStateException If this instance is not active.
     */
    void ensureNotDestroyed() throws IllegalStateException {
        synchronized (this.monitor) {
            if (this.destroying) {
                throw new IllegalStateException(
                    "This instance has already been destroyed.");
            }            
        }
    }

    /**
     * Protected method to ensure the active configuration ID is the same
     * as the default configuration ID.  This will check if they are out
     * of sync and, if so, reinitialize with the default configuration ID.
     * This will attempt to handle the unlikely (though possible) race 
     * conditions by rechecking several times that they are in sync after
     * reinitializing.
     * 
     * @return <code>true</code> if the active configuration ID was updated
     *         to the default configuration ID, otherwise <code>false</code>
     *         if no update was necessary.
     * 
     * @throws SzException If a failure occurs.
     */
    protected boolean ensureConfigCurrent() throws SzException {
        boolean result = false;
        SzConfigManager configMgr = this.getConfigManager();


        // NOTE: there is a possibility for a race condition here
        // where the active config ID or default config ID change
        // after we retrieved them.  If the active config ID changes
        // (presumably to the new default config ID), then 
        // reinitializing should have no effect.  If the default 
        // config ID changes then we will just update again on the 
        // next go around of the loop until the maximum tries exceeded
        for (int tryCount = 0;
             (tryCount <= MAX_REINITIALIZE_COUNT
              && (this.getActiveConfigId() != configMgr.getDefaultConfigId()));
            tryCount++)
        {
            // check if we have exceeded our number of retries
            if (tryCount >= MAX_REINITIALIZE_COUNT) {
                throw new SzException(
                    "Could not reinitialize to the latest default "
                    + "configuration ID after " + tryCount 
                    + " attempts.  activeConfigId=[ " 
                    + this.getActiveConfigId()
                    + " ], defaultConfigId=[ "
                    + configMgr.getDefaultConfigId() + " ]");
            }

            // attempt to reinitialize
            this.reinitialize(
                this.getConfigManager().getDefaultConfigId());

            // set the result to true, but loop through to double-check
            result = true;
        }

        // check if reinitialized
        if (result) {
            synchronized (this.monitor) {
                this.configRefreshCount++;
            }
        }

        // return the result
        return result;
    } 

    /**
     * Gets the {@link RefreshMode} describing how this instance will 
     * handle refreshing the configuration (or not refreshing it).  The
     * mode is set based on the value for the {@linkplain 
     * Builder#getConfigRefreshPeriod() configuration refresh period}
     * provided to the {@link Builder} via {@link 
     * Builder#configRefreshPeriod(Duration)}.
     * 
     * @return The {@link RefreshMode} describing how this instance will
     *         handle refreshing the configuration (or not).
     * 
     * @see #getConfigRefreshPeriod()
     * @see Builder#configRefreshPeriod(Duration)
     * @see Builder#getConfigRefreshPeriod()
     * @see RefreshMode
     */
    public RefreshMode getConfigRefreshMode() {
        Lock lock = null;
        try {
            lock = this.acquireReadLock();
            this.ensureNotDestroyed();
            return this.refreshMode;
        } finally {
            lock = releaseLock(lock);
        }
    }

    /**
     * Gets the {@link Duration} for the {@linkplain 
     * Builder#getConfigRefreshPeriod() configuration refresh period}.
     * 
     * @return The {@link Duration} for the {@linkplain 
     *         Builder#getConfigRefreshPeriod() configuration refresh
     *         period}, or <code>null</code> if configuration refresh
     *         is {@linkplain RefreshMode#DISABLED disabled}.
     * 
     * @see #getConfigRefreshMode()
     * @see Builder#configRefreshPeriod(Duration)
     * @see Builder#getConfigRefreshPeriod()
     * @see RefreshMode
     * 
     */
    public Duration getConfigRefreshPeriod() {
        Lock lock = null;
        try {
            lock = this.acquireReadLock();
            this.ensureNotDestroyed();
            return this.configRefreshPeriod;
        } finally {
            lock = releaseLock(lock);
        }
    }

    /**
     * Overridden to proxy the result to implement the retry-on-refresh
     * logic for methods annotated with {@link SzConfigRetryable} that
     * fail with an {@link SzException}.
     * 
     * {@inheritDoc}
     */
    @Override
    public SzEngine getEngine() throws SzException {
        synchronized (this.monitor) {
            this.ensureNotDestroyed();
            if (this.engine != null) {
                return this.engine;
            }

            SzEngine engine = super.getEngine();
            Class<?>[] interfaces = engine.getClass().getInterfaces();
            ClassLoader classLoader = this.getClass().getClassLoader();
            RetryHandler handler = new RetryHandler(engine);
            this.engine = (SzEngine) Proxy.newProxyInstance(classLoader, interfaces, handler);
        }
        return this.engine;
    }

    /**
     * Overridden to proxy the result to implement the retry-on-refresh
     * logic for methods annotated with {@link SzConfigRetryable} that
     * fail with an {@link SzException}.
     * 
     * {@inheritDoc}
     */
    @Override
    public SzProduct getProduct() throws SzException {
        synchronized (this.monitor) {
            this.ensureNotDestroyed();
            if (this.product != null) {
                return this.product;
            }

            SzProduct product = super.getProduct();
            Class<?>[] interfaces = product.getClass().getInterfaces();
            ClassLoader classLoader = this.getClass().getClassLoader();
            RetryHandler handler = new RetryHandler(product);
            this.product = (SzProduct) Proxy.newProxyInstance(classLoader, interfaces, handler);
        }
        return this.product;
    }

    /**
     * Overridden to proxy the result to implement the retry-on-refresh
     * logic for methods annotated with {@link SzConfigRetryable} that
     * fail with an {@link SzException}.
     * 
     * {@inheritDoc}
     */
    @Override
    public SzConfigManager getConfigManager() throws SzException {
        synchronized (this.monitor) {
            this.ensureNotDestroyed();
            if (this.configManager != null) {
                return this.configManager;
            }

            SzConfigManager configManager = super.getConfigManager();
            Class<?>[] interfaces = configManager.getClass().getInterfaces();
            ClassLoader classLoader = this.getClass().getClassLoader();
            RetryHandler handler = new RetryHandler(configManager);
            this.configManager = (SzConfigManager) 
                Proxy.newProxyInstance(classLoader, interfaces, handler);
        }
        return this.configManager;
    }

    /**
     * Overridden to proxy the result to implement the retry-on-refresh
     * logic for methods annotated with {@link SzConfigRetryable} that
     * fail with an {@link SzException}.
     * 
     * {@inheritDoc}
     */
    @Override
    public SzDiagnostic getDiagnostic() throws SzException {
        synchronized (this.monitor) {
            this.ensureNotDestroyed();
            if (this.diagnostic != null) {
                return this.diagnostic;
            }

            SzDiagnostic diagnostic = super.getDiagnostic();
            Class<?>[] interfaces = diagnostic.getClass().getInterfaces();
            ClassLoader classLoader = this.getClass().getClassLoader();
            RetryHandler handler = new RetryHandler(diagnostic);
            this.diagnostic = (SzDiagnostic) 
                Proxy.newProxyInstance(classLoader, interfaces, handler);
        }
        return this.diagnostic;
    }

    /**
     * Overridden to ensure that if the configuration refresh is 
     * {@linkplain RefreshMode#PROACTIVE proactive} that the background
     * thread that handles periodic refresh is shutdown before destroying
     * the environment.
     * 
     * <p>
     * Further, this override will ensure that all threads in the execution
     * thread pool (if any) are destroyed after the environment is destroyed.
     * </p>
     * 
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        Lock lock = null;
        try {
            synchronized (this.monitor) {
                // check if already destroyed
                if (this.destroying) {
                    return;
                }

                // flag as destroying
                this.destroying = true;
            }

            // check if we have a reinitializer to clean up and shut it down
            // first (even while in-flight operations may be in progress)
            if (this.reinitializer != null) {
                this.reinitializer.complete();

                for (int tryCount = 0;
                     (tryCount < SHUTDOWN_WAIT_COUNT && this.reinitializer.isAlive());
                     tryCount++)
                {
                    try {
                        this.reinitializer.join(SHUTDOWN_WAIT_SECONDS);
                    } catch (InterruptedException e) {
                        // do nothing
                    }
                }
                if (this.reinitializer.isAlive()) {
                    System.err.println("Failed to shutdown reinitializer thread: " 
                                       + this.reinitializer.getName());
                }
            }
            this.reinitializer = null;

            // acquire the write lock to wait for in-flight operations
            // to complete (including compound retry operations)
            lock = this.acquireWriteLock();
            
            // destroy the environment
            super.destroy();

            // set the SDK references to null
            this.engine = null;
            this.configManager = null;
            this.product = null;
            this.diagnostic = null;
            
            // cleanup after the executor
            if (this.coreExecutor != null) {
                this.coreExecutor.shutdown();
                for (int retryCount = 0;
                     (retryCount < SHUTDOWN_WAIT_COUNT && !this.coreExecutor.isTerminated());
                     retryCount++)
                {
                    try {
                        this.coreExecutor.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS);
                    } catch (InterruptedException ignore) {
                        // ignore
                    }
                }
                if (!this.coreExecutor.isTerminated()) {
                    System.err.println("Failed to shutdown executor service: concurrency=[ "
                                       + this.concurrency + " ]");
                }
            }

        } finally {
            lock = releaseLock(lock);
        }
    }


    /**
     * Calls the super class {@link #execute(Callable)} implementation
     * at least once and then repeatedly if necessary until either the
     * call succeeds, fails with an exception other than {@link 
     * SzRetryableException} or the {@linkplain #getMaxBasicRetries()
     * configured maximum} number of retry attempts has been reached.
     * 
     * <p>
     * There will be no delay in making the first retry attempt.
     * Subsequent retry attempts will be attempted after an increasing
     * delay with each attempt.
     * </p>
     * 
     * <p>
     * If still failing with {@link SzRetryableException} after the
     * {@linkplain #getMaxBasicRetries() maximum number retries} has
     * been reached, then the {@link SzRetryableException} is simply
     * re-thrown.
     * </p>
     * 
     * @param <T> The return type of the specified {@link Callable}.
     * 
     * @param task The task to execute.
     * 
     * @return The return value from the specified {@link Callable}
     * 
     * @throws SzRetryableException If the specified task failed with
     *                              this exception and we have retried
     *                              the maximum number of times.
     * 
     * @throws SzException If a failure occurs.
     */
    protected <T> T executeWithBasicRetry(Callable<T> task) 
        throws SzException
    {
        int maxAttempts = this.maxBasicRetries + 1;
        long delayIncrement = 100L;
        SzRetryableException lastException = null;
        for (int index = 0; index < maxAttempts; index++) {
            // delay if retrying more than once
            if (index > 1) {
                long delay = ((index - 1) * delayIncrement);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ignore) {
                    // ignore this exception
                }
            }

            // execute the method and trap any SzRetryableException
            try {
                // flag if we retried
                if (index > 0) {
                    RETRIED_FLAG.set(Boolean.TRUE);
                }

                return super.execute(task);

            } catch (SzRetryableException e) {
                lastException = e;        
            }
        }

        // if we get here then we retried the max number of times
        throw lastException;
    }

    /**
     * Overridden to implement retry logic if {@link #getConfigRefreshMode()}
     * is <b>not</b> {@link RefreshMode#DISABLED} and an {@link SzException}
     * is encountered on a Senzing Core SDK method that is annotated with 
     * {@link SzConfigRetryable}.
     * 
     * {@inheritDoc}
     */
    @Override
    protected <T> T execute(Callable<T> task) 
        throws SzException, IllegalStateException
    {
        Lock lock = null;
        Boolean initialFlag = RETRIED_FLAG.get();
        RETRIED_FLAG.set(Boolean.FALSE); // clear the flag
        try {
            lock = this.acquireReadLock();
            this.ensureNotDestroyed();

            // we need to detect retry failures and count them
            boolean retryFailure = false;

            try {
                // if we are not refreshing the configuration or the
                // retry flag is not set then just execute the task
                if (this.refreshMode == RefreshMode.DISABLED
                    || (!Boolean.TRUE.equals(RETRY_FLAG.get()))) 
                {
                    return this.executeWithBasicRetry(task);
                }

                // otherwise execute and trap any SzException
                try {
                    return this.executeWithBasicRetry(task);
                    
                } catch (SzException e) {
                    // we need to check the active config and 
                    // update the config if necessary
                    boolean configUpdated = false;
                    try {
                        // refresh the configuration
                        configUpdated = ensureConfigCurrent();

                    } catch (SzException e2) {
                        e2.printStackTrace();
                        // if we fail to refresh the config then
                        // rethrow the original exception
                        throw e;
                    }

                    // if the configuration was not updated
                    // then rethrow the original exception
                    if (!configUpdated) {
                        throw e;
                    }

                    // flag that we are retrying
                    RETRIED_FLAG.set(Boolean.TRUE);

                    // if we get here then try again
                    return this.executeWithBasicRetry(task);
                }

            } catch (SzException | RuntimeException e) {
                retryFailure = true;
                throw e;

            } finally {
                // check the retried flag
                if (Boolean.TRUE.equals(RETRIED_FLAG.get())) {
                    synchronized (this.monitor) {
                        this.retriedCount++;
                        if (retryFailure) {
                            this.retriedFailureCount++;
                        }
                    }
                }
                // clear the retried flag
                RETRIED_FLAG.set(initialFlag);
            }

        } finally {
            // release the lock
            lock = releaseLock(lock);
        }
    }

    /**
     * Overridden to implement the use of an internal {@link ExecutorService}
     * if this instance has been configured with a positive concurrency that
     * provides for an execution thread pool.
     * 
     * {@inheritDoc}
     */
    @Override
    protected <T> T doExecute(Callable<T> task) throws Exception
    {
        // check if we have a thread pool
        if (this.coreExecutor == null) {
            return super.doExecute(task);
        }

        // otherwise, use the executor
        Future<T> future = this.coreExecutor.submit(task);

        try {
            // resolve the future
            return future.get();

        } catch (ExecutionException e) {
            // get the cause for the exception
            Throwable cause = e.getCause();

            // check the type
            if (cause instanceof Error) {
                // if an error, rethrow as an error
                throw ((Error) cause);

            } else if (cause instanceof Exception) {
                // if an exception, rethrow as an exception
                throw ((Exception) cause);
                
            } else {
                // for any other throwable, throw the ExecutionException
                throw e;
            }
        }
    }

    /**
     * Performs the specified task using this instance's configured 
     * thread pool and internal {@link ExecutorService} via
     * {@link ExecutorService#submit(Callable)} or directly
     * executes the task in the calling thread if the thread pool 
     * has been disabled.
     * 
     * <p>
     * This returned {@link Future} will provide the result of the
     * task via {@link Future#get()} upon successful completion.
     * </p>
     * 
     * <p>
     * <b>NOTE:</b> Any Senzing Core SDK calls made using this 
     * {@link SzPerpetualCoreEnvironment} instance will also be
     * run in the same thread with no additional context switching.
     * </p>
     * 
     * @param <T> The return type of the task.
     * @param task The {@link Callable} task to perform.
     * @return A {@link Future} representing the result of the
     *         result of the task.
     */
    public <T> Future<T> submitTask(Callable<T> task) {
        Lock lock = null;
        try {
            lock = this.acquireReadLock();
            this.ensureNotDestroyed();

            if (this.coreExecutor == null) {
                try {
                    return completedFuture(task.call());
                } catch (Exception e) {
                    return failedFuture(e);
                }
            } else {
                return this.coreExecutor.submit(task);
            }

        } finally {
            lock = releaseLock(lock);
        }
    }

    /**
     * Performs the specified task using this instance's configured 
     * thread pool and internal {@link ExecutorService} via 
     * {@link ExecutorService#submit(Runnable)} or directly
     * executes the task in the calling thread if the thread pool 
     * has been disabled.
     * 
     * <p>
     * This returned {@link Future} will provide a <code>null</code>
     * value via {@link Future#get()} upon successful completion.
     * </p>
     * 
     * <p>
     * <b>NOTE:</b> Any Senzing Core SDK calls made using this 
     * {@link SzPerpetualCoreEnvironment} instance will also be
     * run in the same thread with no additional context switching.
     * </p>
     * 
     * @param task The {@link Runnable} task to perform.
     * @return A {@link Future} representing the result of the
     *         result of the task that will provide the value
     *         <code>null</code> upon successful completion.
     */
    public Future<?> submitTask(Runnable task) {
        return this.submitTask(task, null);
    }

    /**
     * Performs the specified task using this instance's configured 
     * thread pool and internal {@link ExecutorService} via 
     * {@link ExecutorService#submit(Runnable, Object)} or directly
     * executes the task in the calling thread if the thread pool 
     * has been disabled.
     * 
     * <p>
     * This returned {@link Future} will provide the specified result
     * value via {@link Future#get()} upon successful completion.
     * </p>
     * 
     * <p>
     * <b>NOTE:</b> Any Senzing Core SDK calls made using this 
     * {@link SzPerpetualCoreEnvironment} instance will also be
     * run in the same thread with no additional context switching.
     * </p>
     * 
     * @param <T> The return type of the task.
     * @param task The {@link Callable} task to perform.
     * @param result The result to return from the returned 
     *               {@link Future}.
     * @return A {@link Future} representing the result of the
     *         result of the task.
     */
    public <T> Future<T> submitTask(Runnable task, T result) {
        Lock lock = null;
        try {
            lock = this.acquireReadLock();
            this.ensureNotDestroyed();

            if (this.coreExecutor == null) {
                try {
                    task.run();
                    return completedFuture(result);
                } catch (Exception e) {
                    return failedFuture(e);
                }
            } else {
                return this.coreExecutor.submit(task, result);
            }

        } finally {
            lock = releaseLock(lock);
        }
    }

    /**
     * Acquires an exclusive write lock from this instance's
     * {@link ReentrantReadWriteLock}.
     * 
     * @return The {@link Lock} that was acquired.
     */
    private Lock acquireWriteLock() {
        Lock lock = this.readWriteLock.writeLock();
        lock.lock();
        return lock;
    }

    /**
     * Acquires a shared read lock from this instance's 
     * {@link ReentrantReadWriteLock}.
     * 
     * @return The {@link Lock} that was acquired.
     */
    private Lock acquireReadLock() {
        Lock lock = this.readWriteLock.readLock();
        lock.lock();
        return lock;
    }

    /**
     * Releases the specified {@link Lock} if not <code>null</code>.
     * 
     * @param lock The {@link Lock} to be released.
     * 
     * @return Always returns <code>null</code>.
     */
    private Lock releaseLock(Lock lock) {
        if (lock != null) {
            lock.unlock();
        }
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * <p>
     * Overridden to return <code>true</code> once the {@link #destroy()}
     * method has been called even before the destruction of this instance
     * has completed.
     * </p>
     */
    @Override
    public boolean isDestroyed() {
        synchronized (this.monitor) {
            return this.destroying;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * <p>
     * Overridden to handle timing the blocking of a destroying instance
     * properly.
     * </p>
     */
    @Override
    protected boolean validateActiveInstance() {
        synchronized (this.monitor) {
            if (!this.destroying) {
                return true;
            }
            this.waitUntilDestroyed();
            return false;
        }
    }

    /**
     * Waits until this instance has been destroyed.  This is an internal
     * method used when this instance is being destroyed and we want to
     * wait until it is fully destroyed.
     */
    private void waitUntilDestroyed() 
    {
        synchronized (this.monitor) {
            while (!super.isDestroyed()) {
                try {
                    this.monitor.wait(DESTROY_DELAY);
                } catch (InterruptedException ignore) {
                    // ignore the exception
                }
            }
        }
    }

}
