package com.senzing.sdk.core.perpetual;

import java.util.List;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Random;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

import static java.util.concurrent.TimeUnit.SECONDS;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.Arguments;

import static com.senzing.sdk.core.perpetual.SzPerpetualCoreEnvironment.DISABLED_CONFIG_REFRESH;
import static com.senzing.sdk.core.perpetual.SzPerpetualCoreEnvironment.REACTIVE_CONFIG_REFRESH;
import static com.senzing.sdk.core.perpetual.SzPerpetualCoreEnvironment.RefreshMode.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

import com.senzing.sdk.SzProduct;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.sdk.core.perpetual.SzPerpetualCoreEnvironment.CoreThread;
import com.senzing.text.TextUtilities;
import com.senzing.util.AsyncWorkerPool.Task;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzDiagnostic;
import com.senzing.sdk.SzException;

import static com.senzing.sdk.core.SzCoreEnvironment.*;
import static com.senzing.sdk.test.SdkTest.*;

@TestInstance(Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class SzPerpetualCoreEnvironmentTest extends AbstractPerpetualCoreTest {
    private static final String EMPLOYEES_DATA_SOURCE = "EMPLOYEES";
    
    private static final String CUSTOMERS_DATA_SOURCE = "CUSTOMERS";

    private long configId1 = 0L;

    private long configId2 = 0L;

    private long configId3 = 0L;

    private long defaultConfigId = 0L;

    @BeforeAll public void initializeEnvironment() {
        this.beginTests();
        this.initializeTestEnvironment();
        String settings     = this.getRepoSettings();
        String instanceName = this.getInstanceName();

        SzEnvironment env = SzCoreEnvironment.newBuilder()
                                             .instanceName(instanceName)
                                             .settings(settings)
                                             .verboseLogging(false)
                                             .build();
        
        try {
            String config1 = this.createConfig(env, CUSTOMERS_DATA_SOURCE);
            String config2 = this.createConfig(env, EMPLOYEES_DATA_SOURCE);
            String config3 = this.createConfig(
                env, CUSTOMERS_DATA_SOURCE, EMPLOYEES_DATA_SOURCE);

            SzConfigManager configMgr = env.getConfigManager();
            this.configId1 = configMgr.registerConfig(config1, "Config 1");
            this.configId2 = configMgr.registerConfig(config2, "Config 2");
            this.configId3 = configMgr.registerConfig(config3, "Config 3");

            this.defaultConfigId = configMgr.getDefaultConfigId();

        } catch (Exception e) {
            fail(e);

        } finally {
            env.destroy();
        }   
    }

    @AfterAll public void teardownEnvironment() {
        try {
            this.teardownTestEnvironment();
        } finally {
            this.endTests();
        }
    }

    @Test
    void testNewDefaultBuilder() {
        this.performTest(() -> {    
            SzPerpetualCoreEnvironment env  = null;
            
            try {
                env = SzPerpetualCoreEnvironment.newPerpetualBuilder().build();
    
                assertEquals(0, env.getConcurrency(), 
                    "Environment concurrency is NOT disabled");
                assertEquals(DISABLED, env.getConfigRefreshMode(),
                    "Environment refresh mode is not as expected");
                assertEquals(DISABLED_CONFIG_REFRESH, env.getConfigRefreshPeriod(),
                    "Environment config refresh period is not as expected");
    
            } finally {
                if (env != null) env.destroy();
            }    
        });
    }


    @ParameterizedTest
    @CsvSource({"true,Custom Instance,0,0", "false,Custom Instance,4,2000", "true,  ,0,3000", "false,,6,0"})
    void testNewCustomBuilder(boolean   verboseLogging, 
                              String    instanceName,
                              int       concurrency,
                              long      duration) 
    {
        this.performTest(() -> {
            String settings = this.getRepoSettings();
            
            SzPerpetualCoreEnvironment env  = null;
            
            try {
                SzPerpetualCoreEnvironment.Builder builder 
                    = SzPerpetualCoreEnvironment.newPerpetualBuilder();
                
                Duration configRefreshPeriod = Duration.ofMillis(duration);
                
                env = builder.instanceName(instanceName)
                             .settings(settings)
                             .verboseLogging(verboseLogging)
                             .concurrency(concurrency)
                             .configRefreshPeriod(configRefreshPeriod)
                             .build();

                String expectedName = (instanceName == null || instanceName.trim().length() == 0)
                    ? SzCoreEnvironment.DEFAULT_INSTANCE_NAME : instanceName;
 
                int expectedConcurrency = (concurrency == 0) 
                    ? Runtime.getRuntime().availableProcessors() : concurrency;
                
                assertEquals(expectedName, builder.getInstanceName(),
                        "Builder instance name is not as expected");

                assertEquals(settings, builder.getSettings(),
                        "Builder settings are not as expected");

                assertEquals(verboseLogging, builder.isVerboseLogging(),
                        "Builder verbose logging did not default to false");
                
                assertNull(builder.getConfigId(), "Builder config ID is not null");

                assertEquals(expectedConcurrency, env.getConcurrency(),
                    "Environment concurrency is not as expected");
                
                assertEquals(configRefreshPeriod, env.getConfigRefreshPeriod(),
                        "Environment config refresh period is not as expected");
                    
            } finally {
                if (env != null) env.destroy();
            }    
        });
    }

    @Test
    void testSingletonViolation() {
        this.performTest(() -> {
            SzPerpetualCoreEnvironment env1 = null;
            SzPerpetualCoreEnvironment env2 = null;
            try {
                env1 = (new SzPerpetualCoreEnvironment.Builder()).build();
    
                try {
                    env2 = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                     .settings(DEFAULT_SETTINGS)
                                                     .build();
        
                    // if we get here then we failed
                    fail("Was able to construct a second environment when first was not yet destroyed");
        
                } catch (IllegalStateException expected) {
                    // this exception was expected
                } finally {
                    if (env2 != null) {
                        env2.destroy();
                    }
                }
            } finally {
                if (env1 != null) {
                    env1.destroy();
                }
            }    
        });
    }

    @Test
    void testMixedSingletonViolation() {
        this.performTest(() -> {
            SzCoreEnvironment env1 = null;
            SzPerpetualCoreEnvironment env2 = null;
            try {
                env1 = SzCoreEnvironment.newBuilder().build();
    
                try {
                    env2 = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                     .settings(DEFAULT_SETTINGS)
                                                     .build();
        
                    // if we get here then we failed
                    fail("Was able to construct a second environment when first was not yet destroyed");
        
                } catch (IllegalStateException expected) {
                    // this exception was expected
                } finally {
                    if (env2 != null) {
                        env2.destroy();
                    }
                }
            } finally {
                if (env1 != null) {
                    env1.destroy();
                }
            }    
        });
    }

    @Test
    void testSingletonAdherence() {
        this.performTest(() -> {
            SzPerpetualCoreEnvironment env1 = null;
            SzPerpetualCoreEnvironment env2 = null;
            try {
                env1 = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                 .instanceName("Instance 1")
                                                 .build();
    
                env1.destroy();
                env1 = null;
    
                env2 = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                 .instanceName("Instance 2")
                                                 .settings(DEFAULT_SETTINGS)
                                                 .build();
    
                env2.destroy();
                env2 = null;
    
            } finally {
                if (env1 != null) {
                    env1.destroy();
                }
                if (env2 != null) {
                    env2.destroy();
                }
            }    
        });
    }

    @Test
    void testMixedSingletonAdherence() {
        this.performTest(() -> {
            SzCoreEnvironment env1 = null;
            SzPerpetualCoreEnvironment env2 = null;
            try {
                env1 = SzCoreEnvironment.newBuilder()
                                        .instanceName("Instance 1")
                                        .build();
    
                env1.destroy();
                env1 = null;
    
                env2 = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                 .instanceName("Instance 2")
                                                 .settings(DEFAULT_SETTINGS)
                                                 .build();
    
                env2.destroy();
                env2 = null;
    
            } finally {
                if (env1 != null) {
                    env1.destroy();
                }
                if (env2 != null) {
                    env2.destroy();
                }
            }    
        });
    }

    @Test
    void testDestroy() {
        this.performTest(() -> {
            SzPerpetualCoreEnvironment env1 = null;
            SzPerpetualCoreEnvironment env2 = null;
            try {
                // get the first environment
                env1 = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                 .instanceName("Instance 1")
                                                 .build();
    
                // ensure it is active
                assertFalse(env1.isDestroyed(),  "First Environment instance is destroyed.");
    
                // destroy the first environment
                env1.destroy();
    
                // check it is now inactive
                assertTrue(env1.isDestroyed(), "First Environment instance is still active.");
    
                // clear the env1 reference
                env1 = null;
                
                // create a second environment instance
                env2 = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                 .instanceName("Instance 2")
                                                 .settings(DEFAULT_SETTINGS)
                                                 .build();
    
                // ensure it is active
                assertFalse(env2.isDestroyed(),  "Second Environment instance is destroyed.");
    
                // destroy the first environment
                env2.destroy();
    
                // check it is now inactive
                assertTrue(env2.isDestroyed(), "Second Environment instance is still active.");
    
                env2 = null;
    
            } finally {
                if (env1 != null) {
                    env1.destroy();
                }
                if (env2 != null) {
                    env2.destroy();
                }
            }    
        });
    }

    /**
     * Extends {@link Thread} to allow for identifying of core threads.
     */
    static class CallingThread extends Thread {
        /**
         * Constructs with the specified {@link Runnable}.
         * 
         * @param runnable The {@link Runnable} with which to construct with.
         */
        public CallingThread(Runnable runnable) {
            super(runnable);
        }
    }

    private static final ThreadFactory THREAD_FACTORY = (r) -> new CallingThread(r);

    @ParameterizedTest
    @CsvSource({"1, 1, Foo", "1, 0, Foo", "2, 0, Bar", "2, 1, Bar", "2, 2, Bar", 
                "3, 0, Phoo", "3, 1, Phoo", "3, 2, Phoo", "4, 0, Phoox", "4, 1, Phoox",
                "4, 2, Phoox", "4, 3, Phoox", "4, 4, Phoox"})
    void testExecute(int threadCount, int concurrencyParam, String expected) {
        Integer concurrency = (concurrencyParam == 0) ? null : concurrencyParam;
        this.performTest(() -> {
            SzPerpetualCoreEnvironment env  = null;

            final IdentityHashMap<Thread,Integer> threadMap = new IdentityHashMap<>();

            ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                threadCount, threadCount, 10L, SECONDS, 
                new LinkedBlockingQueue<>(), THREAD_FACTORY);
    
            List<Future<String>> futures = new ArrayList<>(threadCount);
            try {
                env  = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                 .concurrency(concurrency)
                                                 .build();
    
                final SzPerpetualCoreEnvironment environment = env;
    
                Callable<String> task = () -> {
                    synchronized (threadMap) {
                        Thread currentThread = Thread.currentThread();
                        Integer count = threadMap.get(currentThread);
                        if (count == null) { 
                            count = 1;
                        } else {
                            count = count + 1;
                        }
                        threadMap.put(currentThread, count);
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignore) {
                        // do nothing
                    }
                    return expected;
                };

                // loop through the threads
                for (int index = 0; index < threadCount; index++) {
                    Future<String> future = threadPool.submit(() -> {
                        return environment.execute(task);
                    });
                    futures.add(future);
                }

                // execute the same tasks from this thread
                for (int index = 0; index < threadCount; index++) {
                    try {
                        String actual = environment.execute(task);

                        assertEquals(expected, actual, "Unexpected result from execute()");

                    } catch (Exception e) {
                        fail("Failed with unexpected exception", e);
                    }
                }
                
                // loop through the futures
                for (Future<String> future : futures) {
                    try {
                        String actual = future.get();
                        assertEquals(expected, actual, "Unexpected result from execute()");
                    } catch (Exception e) {
                        fail("Failed execute with exception", e);
                    }
                }

                // check the thread map
                if (concurrency == null) {
                    assertTrue(threadMap.containsKey(Thread.currentThread()),
                                "Tasks were unexpectedly executed in a thread other "
                                + "than the calling thread");

                    assertEquals(threadCount, threadMap.get(Thread.currentThread()),
                                 "Unexpected number of tasks executed in primary calling thread");

                    assertEquals(threadCount, 
                                 threadMap.entrySet().stream()
                                          .filter((e) -> (e.getKey() instanceof CallingThread))
                                          .mapToInt(e -> e.getValue()).sum(),
                                 "Unexpected number of tasks executed in pooled calling threads");

                    assertTrue(threadMap.keySet().stream().noneMatch(thread -> thread instanceof CoreThread), 
                               "At least some tasks were executed in a CoreThread");

                    assertTrue(threadMap.keySet().stream().allMatch(
                               thread -> (thread instanceof CallingThread || thread == Thread.currentThread())), 
                               "At least some tasks were executed in an unexpected thread");
                        
                } else {
                    assertTrue(threadMap.keySet().stream().noneMatch(
                               thread -> (thread instanceof CallingThread || thread == Thread.currentThread())), 
                               "At least some tasks were executed in a calling thread");

                    assertTrue(threadMap.keySet().stream().allMatch(thread -> thread instanceof CoreThread), 
                               "At least some tasks were executed outside a CoreThread");

                    assertEquals(threadCount * 2, 
                                 threadMap.entrySet().stream().mapToInt(e -> e.getValue()).sum(),
                                 "Unexpected number of tasks executed in core execution threads");
                }
                threadMap.clear();

    
            } finally {
                if (env != null) {
                    env.destroy();
                }
            }    
        });
    }

    @ParameterizedTest
    @CsvSource({"1, 1, Foo", "1, 0, Foo", "2, 0, Bar", "2, 1, Bar", "2, 2, Bar", 
                "3, 0, Phoo", "3, 1, Phoo", "3, 2, Phoo", "4, 0, Phoox", "4, 1, Phoox",
                "4, 2, Phoox", "4, 3, Phoox", "4, 4, Phoox"})
    void testSubmitTaskCallable(int threadCount, int concurrencyParam, String expected) {
        Integer concurrency = (concurrencyParam == 0) ? null : concurrencyParam;
        this.performTest(() -> {
            SzPerpetualCoreEnvironment env  = null;

            final IdentityHashMap<Thread,Integer> threadMap = new IdentityHashMap<>();

            ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                threadCount, threadCount, 10L, SECONDS, 
                new LinkedBlockingQueue<>(), THREAD_FACTORY);
    
            List<Future<String>> futures = new ArrayList<>(threadCount);
            try {
                env  = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                 .concurrency(concurrency)
                                                 .build();
    
                final SzPerpetualCoreEnvironment environment = env;
    
                Callable<String> task = () -> {
                    synchronized (threadMap) {
                        Thread currentThread = Thread.currentThread();
                        Integer count = threadMap.get(currentThread);
                        if (count == null) { 
                            count = 1;
                        } else {
                            count = count + 1;
                        }
                        threadMap.put(currentThread, count);
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignore) {
                        // do nothing
                    }
                    return expected;
                };

                // loop through the threads
                for (int index = 0; index < threadCount; index++) {
                    Future<String> future = threadPool.submit(() -> {
                         Future<String> subFuture = environment.submitTask(task);
                         return subFuture.get();
                    });
                    futures.add(future);
                }

                // execute the same tasks from this thread
                for (int index = 0; index < threadCount; index++) {
                    try {
                        Future<String> future = environment.submitTask(task);
                        futures.add(future);

                    } catch (Exception e) {
                        fail("Failed with unexpected exception", e);
                    }
                }
                
                // loop through the futures
                for (Future<String> future : futures) {
                    try {
                        String actual = future.get();
                        assertEquals(expected, actual, "Unexpected result from execute()");
                    } catch (Exception e) {
                        fail("Failed execute with exception", e);
                    }
                }

                // shutdown the thread pool and environment
                threadPool.shutdown();
                while (!threadPool.isTerminated()) {
                    try {
                        threadPool.awaitTermination(100, SECONDS);
                    } catch (InterruptedException ignore) {
                        // do nothing
                    }
                }
                env.destroy();
                env = null;

                // check the thread map
                if (concurrency == null) {
                    assertTrue(threadMap.containsKey(Thread.currentThread()),
                                "Tasks were unexpectedly executed in a thread other "
                                + "than the calling thread");

                    assertEquals(threadCount, threadMap.get(Thread.currentThread()),
                                 "Unexpected number of tasks executed in primary calling thread");

                    assertEquals(threadCount, 
                                 threadMap.entrySet().stream()
                                          .filter((e) -> (e.getKey() instanceof CallingThread))
                                          .mapToInt(e -> e.getValue()).sum(),
                                 "Unexpected number of tasks executed in pooled calling threads");

                    assertTrue(threadMap.keySet().stream().noneMatch(thread -> thread instanceof CoreThread), 
                               "At least some tasks were executed in a CoreThread");

                    assertTrue(threadMap.keySet().stream().allMatch(
                               thread -> (thread instanceof CallingThread || thread == Thread.currentThread())), 
                               "At least some tasks were executed in an unexpected thread");
                        
                } else {
                    assertTrue(threadMap.keySet().stream().noneMatch(
                               thread -> (thread instanceof CallingThread || thread == Thread.currentThread())), 
                               "At least some tasks were executed in a calling thread");

                    assertTrue(threadMap.keySet().stream().allMatch(thread -> thread instanceof CoreThread), 
                               "At least some tasks were executed outside a CoreThread");

                    assertEquals(threadCount * 2, 
                                 threadMap.entrySet().stream().mapToInt(e -> e.getValue()).sum(),
                                 "Unexpected number of tasks executed in core execution threads");
                }
                threadMap.clear();

    
            } finally {
                if (env != null) {
                    env.destroy();
                }
            }    
        });
    }

    @ParameterizedTest
    @CsvSource({"1, 1, Foo", "1, 0, Foo", "2, 0, Bar", "2, 1, Bar", "2, 2, Bar", 
                "3, 0, Phoo", "3, 1, Phoo", "3, 2, Phoo", "4, 0, Phoox", "4, 1, Phoox",
                "4, 2, Phoox", "4, 3, Phoox", "4, 4, Phoox"})
    void testSubmitTaskRunnableResult(int threadCount, int concurrencyParam, String expected) {
        Integer concurrency = (concurrencyParam == 0) ? null : concurrencyParam;
        this.performTest(() -> {
            SzPerpetualCoreEnvironment env  = null;

            final IdentityHashMap<Thread,Integer> threadMap = new IdentityHashMap<>();

            ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                threadCount, threadCount, 10L, SECONDS, 
                new LinkedBlockingQueue<>(), THREAD_FACTORY);
    
            List<Future<String>> futures = new ArrayList<>(threadCount);
            try {
                env  = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                 .concurrency(concurrency)
                                                 .build();
    
                final SzPerpetualCoreEnvironment environment = env;
    
                Runnable task = () -> {
                    synchronized (threadMap) {
                        Thread currentThread = Thread.currentThread();
                        Integer count = threadMap.get(currentThread);
                        if (count == null) { 
                            count = 1;
                        } else {
                            count = count + 1;
                        }
                        threadMap.put(currentThread, count);
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignore) {
                        // do nothing
                    }
                };

                // loop through the threads
                for (int index = 0; index < threadCount; index++) {
                    Future<String> future = threadPool.submit(() -> {
                         Future<String> subFuture = environment.submitTask(task, expected);
                         return subFuture.get();
                    });
                    futures.add(future);
                }

                // execute the same tasks from this thread
                for (int index = 0; index < threadCount; index++) {
                    try {
                        Future<String> future = environment.submitTask(task, expected);
                        futures.add(future);
                        
                    } catch (Exception e) {
                        fail("Failed with unexpected exception", e);
                    }
                }
                
                // loop through the futures
                for (Future<String> future : futures) {
                    try {
                        String actual = future.get();
                        assertEquals(expected, actual, "Unexpected result from execute()");
                    } catch (Exception e) {
                        fail("Failed execute with exception", e);
                    }
                }

                // shutdown the thread pool and environment
                threadPool.shutdown();
                while (!threadPool.isTerminated()) {
                    try {
                        threadPool.awaitTermination(100, SECONDS);
                    } catch (InterruptedException ignore) {
                        // do nothing
                    }
                }
                env.destroy();
                env = null;

                // check the thread map
                if (concurrency == null) {
                    assertTrue(threadMap.containsKey(Thread.currentThread()),
                                "Tasks were unexpectedly executed in a thread other "
                                + "than the calling thread");

                    assertEquals(threadCount, threadMap.get(Thread.currentThread()),
                                 "Unexpected number of tasks executed in primary calling thread");

                    assertEquals(threadCount, 
                                 threadMap.entrySet().stream()
                                          .filter((e) -> (e.getKey() instanceof CallingThread))
                                          .mapToInt(e -> e.getValue()).sum(),
                                 "Unexpected number of tasks executed in pooled calling threads");

                    assertTrue(threadMap.keySet().stream().noneMatch(thread -> thread instanceof CoreThread), 
                               "At least some tasks were executed in a CoreThread");

                    assertTrue(threadMap.keySet().stream().allMatch(
                               thread -> (thread instanceof CallingThread || thread == Thread.currentThread())), 
                               "At least some tasks were executed in an unexpected thread");
                        
                } else {
                    assertTrue(threadMap.keySet().stream().noneMatch(
                               thread -> (thread instanceof CallingThread || thread == Thread.currentThread())), 
                               "At least some tasks were executed in a calling thread");

                    assertTrue(threadMap.keySet().stream().allMatch(thread -> thread instanceof CoreThread), 
                               "At least some tasks were executed outside a CoreThread");

                    assertEquals(threadCount * 2, 
                                 threadMap.entrySet().stream().mapToInt(e -> e.getValue()).sum(),
                                 "Unexpected number of tasks executed in core execution threads");
                }
                threadMap.clear();

    
            } finally {
                if (env != null) {
                    env.destroy();
                }
            }    
        });
    }

    @ParameterizedTest
    @CsvSource({"1, 1", "1, 0", "2, 0", "2, 1", "2, 2", "3, 0", "3, 1", 
                "3, 2", "4, 0", "4, 1", "4, 2", "4, 3", "4, 4"})
    void testSubmitTaskRunnable(int threadCount, int concurrencyParam) {
        Integer concurrency = (concurrencyParam == 0) ? null : concurrencyParam;
        this.performTest(() -> {
            SzPerpetualCoreEnvironment env  = null;

            final IdentityHashMap<Thread,Integer> threadMap = new IdentityHashMap<>();

            ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                threadCount, threadCount, 10L, SECONDS, 
                new LinkedBlockingQueue<>(), THREAD_FACTORY);
    
            try {
                env  = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                 .concurrency(concurrency)
                                                 .build();
    
                final SzPerpetualCoreEnvironment environment = env;
    
                Runnable task = () -> {
                    synchronized (threadMap) {
                        Thread currentThread = Thread.currentThread();
                        Integer count = threadMap.get(currentThread);
                        if (count == null) { 
                            count = 1;
                        } else {
                            count = count + 1;
                        }
                        threadMap.put(currentThread, count);
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignore) {
                        // do nothing
                    }
                };

                // loop through the threads
                for (int index = 0; index < threadCount; index++) {
                    threadPool.submit(() -> {
                        environment.submitTask(task);
                    });
                }

                // execute the same tasks from this thread
                for (int index = 0; index < threadCount; index++) {
                    try {
                        environment.submitTask(task);
                        
                    } catch (Exception e) {
                        fail("Failed with unexpected exception", e);
                    }
                }

                // shutdown the thread pool and environment
                threadPool.shutdown();
                while (!threadPool.isTerminated()) {
                    try {
                        threadPool.awaitTermination(100, SECONDS);
                    } catch (InterruptedException ignore) {
                        // do nothing
                    }
                }
                env.destroy();
                env = null;

                // check the thread map
                if (concurrency == null) {
                    assertTrue(threadMap.containsKey(Thread.currentThread()),
                                "Tasks were unexpectedly executed in a thread other "
                                + "than the calling thread");

                    assertEquals(threadCount, threadMap.get(Thread.currentThread()),
                                 "Unexpected number of tasks executed in primary calling thread");

                    assertEquals(threadCount, 
                                 threadMap.entrySet().stream()
                                          .filter((e) -> (e.getKey() instanceof CallingThread))
                                          .mapToInt(e -> e.getValue()).sum(),
                                 "Unexpected number of tasks executed in pooled calling threads");

                    assertTrue(threadMap.keySet().stream().noneMatch(thread -> thread instanceof CoreThread), 
                               "At least some tasks were executed in a CoreThread");

                    assertTrue(threadMap.keySet().stream().allMatch(
                               thread -> (thread instanceof CallingThread || thread == Thread.currentThread())), 
                               "At least some tasks were executed in an unexpected thread");
                        
                } else {
                    assertTrue(threadMap.keySet().stream().noneMatch(
                               thread -> (thread instanceof CallingThread || thread == Thread.currentThread())), 
                               "At least some tasks were executed in a calling thread");

                    assertTrue(threadMap.keySet().stream().allMatch(thread -> thread instanceof CoreThread), 
                               "At least some tasks were executed outside a CoreThread");

                    assertEquals(threadCount * 2, 
                                 threadMap.entrySet().stream().mapToInt(e -> e.getValue()).sum(),
                                 "Unexpected number of tasks executed in core execution threads");
                }
                threadMap.clear();

    
            } finally {
                if (env != null) {
                    env.destroy();
                }
            }    
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"Foo", "Bar", "Phoo", "Phoox"})
    void testExecuteFail(String expected) {
        this.performTest(() -> {
            SzPerpetualCoreEnvironment env  = null;
            try {
                env  = SzPerpetualCoreEnvironment.newPerpetualBuilder().build();
    
                try {
                   env.execute(() -> {
                        throw new SzException(expected);
                   });
    
                   fail("Expected SzException was not thrown");
    
                } catch (SzException e) {
                    assertEquals(expected, e.getMessage(), "Unexpected exception messasge");
    
                } catch (Exception e) {
                    fail("Failed execute with exception", e);
                }
    
            } finally {
                if (env != null) {
                    env.destroy();
                }
            }    
        });
    }

    @Test
    void testDestroyRaceConditions() {
        this.performTest(() -> {
            SzPerpetualCoreEnvironment env = SzPerpetualCoreEnvironment.newPerpetualBuilder().build();

            final Object monitor = new Object();
            final Exception[] failures = { null, null, null };
            Thread busyThread = new Thread(() -> {
                try {
                    env.execute(() -> {
                        synchronized (monitor) {
                            monitor.wait(15000L);
                        }
                        return null;
                    });
                } catch (Exception e) {
                    failures[0] = e;
                }
            });

            Long[] destroyDuration = { null };
            Thread destroyThread = new Thread(() -> {
                try {
                    Thread.sleep(100L);
                    long start = System.nanoTime();
                    env.destroy();
                    long end = System.nanoTime();
        
                    destroyDuration[0] = (end - start) / 1000000L;
                } catch (Exception e) {
                    failures[1] = e;
                }
            });

            // start the thread that will keep the environment busy
            busyThread.start();

            // start the thread that will destroy the environment
            destroyThread.start();

            // sleep for one second to ensure destroy has been called
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                fail("Interrupted sleep delay", e);
            }

            boolean destroyed = env.isDestroyed();
            assertTrue(destroyed, "Environment NOT marked as destroyed");
            
            SzCoreEnvironment active = SzCoreEnvironment.getActiveInstance();

            assertNull(active, "Active instance was NOT null when destroying");

            // try to execute after destroy
            try {
                env.execute(() -> {
                    return null;
                });
                fail("Unexpectedly managed to execute on a destroyed instance");

            } catch (IllegalStateException expected) {
                // all is well
            } catch (Exception e) {
                fail("Failed with unexpected exception", e);
            }

            try {
                busyThread.join();
                destroyThread.join();
            } catch (Exception e) {
                fail("Thread joining failed with an exception.", e);
            }

            assertNotNull(destroyDuration[0], "Destroy duration was not record");
            assertTrue(destroyDuration[0] > 2000L, "Destroy occurred too quickly: " 
                        + destroyDuration[0] + "ms");

            if (failures[0] != null) {
                fail("Busy thread got an exception.", failures[0]);
            }
            if (failures[1] != null) {
                fail("Destroying thread got an exception.", failures[1]);
            }

        });
    }

    @Test
    void testGetActiveInstance() {
        this.performTest(() -> {
            SzPerpetualCoreEnvironment env1 = null;
            SzPerpetualCoreEnvironment env2 = null;
            try {
                // get the first environment
                env1 = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                 .instanceName("Instance 1")
                                                 .build();
    
                SzCoreEnvironment active = SzCoreEnvironment.getActiveInstance();

                assertNotNull(active, "No active instance found when it should have been: " 
                              + env1);
                assertSame(env1, active,
                            "Active instance was not as expected: " + active);
    
                // destroy the first environment
                env1.destroy();
    
                active = SzCoreEnvironment.getActiveInstance();
                assertNull(active,
                           "Active instance found when there should be none: " + active);
                            
                // create a second Environment instance
                env2 = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                 .instanceName("Instance 2")
                                                 .settings(DEFAULT_SETTINGS)
                                                 .build();
    
                active = SzCoreEnvironment.getActiveInstance();
                assertNotNull(active, "No active instance found when it should have been: " 
                              + env2);
                assertSame(env2, active,
                           "Active instance was not as expected: " + active);
                    
                // destroy the second environment
                env2.destroy();
    
                active = SzCoreEnvironment.getActiveInstance();
                assertNull(active,
                    "Active instance found when there should be none: " + active);
                
                env2 = null;
    
            } finally {
                if (env1 != null) {
                    env1.destroy();
                }
                if (env2 != null) {
                    env2.destroy();
                }
            }    
       });
    }

    @Test
    void testGetConfigManager() {
        this.performTest(() -> {
            String settings = this.getRepoSettings();
            
            SzPerpetualCoreEnvironment env  = null;
            
            try {
                env = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                .instanceName("GetConfigManager Instance")
                                                .settings(settings)
                                                .verboseLogging(false)
                                                .configRefreshPeriod(REACTIVE_CONFIG_REFRESH)
                                                .build();

                SzConfigManager configMgr1 = env.getConfigManager();
                SzConfigManager configMgr2 = env.getConfigManager();

                assertNotNull(configMgr1, "SzConfigManager was null");
                assertSame(configMgr1, configMgr2, "SzConfigManager not returning the same object");

                env.destroy();
                env  = null;

            } catch (SzException e) {
                fail("Got SzException during test", e);

            } finally {
                if (env != null) env.destroy();
            }    
        });
    }

    @Test
    void testGetDiagnostic() {
        this.performTest(() -> {
            String settings = this.getRepoSettings();
            
            SzCoreEnvironment env  = null;
            
            try {
                env = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                .instanceName("GetDiagnostic Instance")
                                                .settings(settings)
                                                .verboseLogging(false)
                                                .configRefreshPeriod(REACTIVE_CONFIG_REFRESH)
                                                .build();

                SzDiagnostic diagnostic1 = env.getDiagnostic();
                SzDiagnostic diagnostic2 = env.getDiagnostic();

                assertNotNull(diagnostic1, "SzDiagnostic was null");
                assertSame(diagnostic1, diagnostic2, "SzDiagnostic not returning the same object");

                env.destroy();
                env  = null;

            } catch (SzException e) {
                fail("Got SzException during test", e);

            } finally {
                if (env != null) env.destroy();
            }    
        });
    }

    @Test
    void testGetEngine() {
        this.performTest(() -> {
            String settings = this.getRepoSettings();
            
            SzCoreEnvironment env  = null;
            
            try {
                env = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                .instanceName("GetEngine Instance")
                                                .settings(settings)
                                                .verboseLogging(false)
                                                .configRefreshPeriod(REACTIVE_CONFIG_REFRESH)
                                                .build();

                SzEngine engine1 = env.getEngine();
                SzEngine engine2 = env.getEngine();

                assertNotNull(engine1, "SzEngine was null");
                assertSame(engine1, engine2, "SzEngine not returning the same object");

                env.destroy();
                env  = null;

            } catch (SzException e) {
                fail("Got SzException during test", e);

            } finally {
                if (env != null) env.destroy();
            }    
        });
    }

    @Test
    void testGetProduct() {
        this.performTest(() -> {
            this.performTest(() -> {
                String settings = this.getRepoSettings();
            
                SzCoreEnvironment env  = null;
                
                try {
                    env = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                    .instanceName("GetProduct Instance")
                                                    .settings(settings)
                                                    .verboseLogging(false)
                                                    .configRefreshPeriod(REACTIVE_CONFIG_REFRESH)
                                                    .build();
    
                    SzProduct product1 = env.getProduct();
                    SzProduct product2 = env.getProduct();
    
                    assertNotNull(product1, "SzProduct was null");
                    assertSame(product1, product2, "SzProduct not returning the same object");

                    env.destroy();
                    env  = null;
    
                } catch (SzException e) {
                    fail("Got SzException during test", e);
    
                } finally {
                    if (env != null) env.destroy();
                }        
            });
        });
    }

    private List<Arguments> getActiveConfigIdParams() {
        List<Arguments> result = new LinkedList<>();
        long[] configIds = { this.configId1, this.configId2, this.configId3 };

        boolean initEngine = false;
        for (long config : configIds) {
            initEngine = !initEngine;
            result.add(Arguments.of(config, initEngine));
        }

        return result;
    }

    @ParameterizedTest
    @MethodSource("getActiveConfigIdParams")
    public void testGetActiveConfigId(long configId, boolean initEngine) {
        this.performTest(() -> {
            SzPerpetualCoreEnvironment env  = null;

            String info = "configId=[ " + configId + " ], initEngine=[ " 
                    + initEngine + " ]";
                
            try {
                String settings = this.getRepoSettings();
                String instanceName = this.getInstanceName(
                    "ActiveConfig-" + configId);
    
                env = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                .settings(settings)
                                                .instanceName(instanceName)
                                                .configId(configId)
                                                .build();
    
                // get the active config
                long activeConfigId = env.getActiveConfigId();
    
                assertEquals(configId, activeConfigId,
                    "The active config ID is not as expected: " + info);
                        
            } catch (Exception e) {
                fail("Got exception in testGetActiveConfigId: " + info, e);
    
            } finally {
                if (env != null) env.destroy();
            }    
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false})
    public void testGetActiveConfigIdDefault(boolean initEngine) {
        this.performTest(() -> {
            SzPerpetualCoreEnvironment env  = null;
        
            String info = "initEngine=[ " + initEngine + " ]";
    
            try {
                String settings = this.getRepoSettings();
                String instanceName = this.getInstanceName(
                    "ActiveConfigDefault");
                    
                env = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                .settings(settings)
                                                .instanceName(instanceName)
                                                .build();
    
                // get the active config
                long activeConfigId = env.getActiveConfigId();
    
                assertEquals(this.defaultConfigId, activeConfigId,
                    "The active config ID is not as expected: " + info);
                        
            } catch (Exception e) {
                fail("Got exception in testGetActiveConfigIdDefault: " + info, e);
                
            } finally {
                if (env != null) env.destroy();
            }    
        });
    }

    private List<Arguments> getReinitializeParams() {
        List<Arguments> result = new LinkedList<>();
        List<List<Boolean>> booleanCombos = getBooleanVariants(2, false);

        Random prng = new Random(System.currentTimeMillis());


        
        List<Long> configIds = List.of(this.configId1, this.configId2, this.configId3);
        List<List<?>> configIdCombos = generateCombinations(configIds, configIds);

        Collections.shuffle(configIdCombos, prng);
        Collections.shuffle(booleanCombos, prng);

        Iterator<List<?>> configIdIter = circularIterator(configIdCombos);


        for (List<Boolean> bools : booleanCombos) {
            boolean initEngine = bools.get(0);
            boolean initDiagnostic = bools.get(1);

            for (Long configId : configIds) {
                result.add(Arguments.of(null, configId, initEngine, initDiagnostic));
            }

            List<?> configs = configIdIter.next();
            result.add(Arguments.of(configs.get(0), 
                                    configs.get(1),
                                    initEngine,
                                    initDiagnostic));
        }

        return result;
    }

    @Test
    public void testExecuteException() {
        this.performTest(() -> {
            SzPerpetualCoreEnvironment env = SzPerpetualCoreEnvironment.newPerpetualBuilder().build();
            try {
                env.execute(() -> {
                    throw new IOException("Test exception");
                });
            } catch (SzException e) {
                Throwable cause = e.getCause();
                assertInstanceOf(IOException.class, cause, "The cause was not an IOException");
            } catch (Exception e) {
                fail("Caught an unexpected exeption", e);
            } finally {
                if (env != null) env.destroy();
            }
        });
    }

    @ParameterizedTest
    @MethodSource("getReinitializeParams")
    public void testReinitialize(Long       startConfig, 
                                 Long       endConfig,
                                 boolean    initEngine, 
                                 boolean    initDiagnostic) 
    {
        this.performTest(() -> {
            SzPerpetualCoreEnvironment env = null;
            
            String info = "startConfig=[ " + startConfig + " ], endConfig=[ " 
                 + endConfig + " ], initEngine=[ " + initEngine
                 + " ], initDiagnostic=[ " + initDiagnostic + " ]";
    
            try {
                String settings = this.getRepoSettings();
                String instanceName = this.getInstanceName("Reinitialize");
    
                env = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                                .settings(settings)
                                                .instanceName(instanceName)
                                                .configId(startConfig)
                                                .build();
        
                // check if we should initialize the engine first
                if (initEngine) env.getEngine();
    
                // check if we should initialize diagnostics first
                if (initDiagnostic) env.getDiagnostic();
    
                Long activeConfigId = null;
                if (startConfig != null) {
                    // get the active config
                    activeConfigId = env.getActiveConfigId();
    
                    assertEquals(startConfig, activeConfigId,
                        "The starting active config ID is not as expected: " + info);
                }
    
                // reinitialize
                env.reinitialize(endConfig);
            
                // check the initialize config ID
                activeConfigId = env.getActiveConfigId();
    
                assertEquals(endConfig, activeConfigId,
                    "The ending active config ID is not as expected: " + info);
    
            } catch (Exception e) {
                fail("Got exception in testReinitialize: " + info, e);
    
            } finally {
                if (env != null) env.destroy();
            }    
        });
    }

    private static class MockEnvironment extends SzPerpetualCoreEnvironment {
        public MockEnvironment(String instanceName, String settings) {
            super(SzPerpetualCoreEnvironment.newPerpetualBuilder()
                    .settings(settings).instanceName(instanceName)
                    .configRefreshPeriod(REACTIVE_CONFIG_REFRESH));
        }

        protected boolean ensureConfigCurrent() throws SzException {
            super.ensureConfigCurrent();
            return true;
        }
    }

    public static class MockRetryCallable implements Callable<Integer> {
        private List<Boolean> retryList = null;
        private String errorMessage = null;
        
        public MockRetryCallable(boolean succeedOnRetry) 
        {
            this(succeedOnRetry,
                 TextUtilities.randomAlphanumericText(20));
        }

        public MockRetryCallable(boolean    succeedOnRetry, 
                                 String     errorMessage) 
        {
            this.retryList = new LinkedList<>();
            this.retryList.add(Boolean.FALSE);
            this.retryList.add(succeedOnRetry);
            this.errorMessage   = errorMessage;
        }

        public String getErrorMessage() {
            return this.errorMessage;
        }

        @Override
        public Integer call() throws Exception {
            Boolean succeed = this.retryList.remove(0);
            if (!succeed) {
                // use the list size as mock error code
                throw new SzException(this.retryList.size(), this.errorMessage);
            }
            return this.retryList.size();
        }
    }

    @Test
    public void mockRetryTest() {
        this.performTest(() -> {
            SzPerpetualCoreEnvironment env = null;
    
            try {
                String settings = this.getRepoSettings();
                String instanceName = this.getInstanceName("Mock Retry Test");
    
                env = new MockEnvironment(instanceName, settings);

                // ensure we succeed if succeed on retry
                MockRetryCallable mrc = new MockRetryCallable(true);
                try {
                    SzPerpetualCoreEnvironment.RETRY_FLAG.set(Boolean.TRUE);
                    Integer result = env.execute(mrc);

                    assertEquals(0, result, 
                        "Unexpectedly succeeded on first try");

                } catch (SzException e) {
                    fail("Failed retry when expected successs on retry: " + e.getErrorCode(), 
                         e);

                } finally {
                    SzPerpetualCoreEnvironment.RETRY_FLAG.set(Boolean.FALSE);
                }

                // ensure we fail if failing on retry
                mrc = new MockRetryCallable(false);
                try {
                    SzPerpetualCoreEnvironment.RETRY_FLAG.set(Boolean.TRUE);
                    Integer result = env.execute(mrc);

                    fail("Unexpectedly succeeded when we should have failed on retry: " 
                         + result);

                } catch (SzException expected) {
                    assertEquals(0, expected.getErrorCode(),
                                 "Retry failed as expected, but unexpectedly threw "
                                 + "the original exception");
                    assertEquals(mrc.getErrorMessage(), expected.getMessage(),
                                 "Retry failed as expected, but not with the expected "
                                 + "error message.");

                } finally {
                    SzPerpetualCoreEnvironment.RETRY_FLAG.set(Boolean.FALSE);
                }

                // ensure we fail if not retrying
                mrc = new MockRetryCallable(true);
                try {
                    SzPerpetualCoreEnvironment.RETRY_FLAG.set(Boolean.FALSE);
                    Integer result = env.execute(mrc);

                    fail("Unexpectedly succeeded when we should have "
                         + "failed with no retry: " + result);

                } catch (SzException expected) {
                    assertEquals(1, expected.getErrorCode(),
                                "Task without retry failed as expected, but did not "
                                + "throw the an exception on first try");
                    assertEquals(mrc.getErrorMessage(), expected.getMessage(),
                                "Task without retry failed as expected, but not with "
                                + "the expected error message.");

                } finally {
                    SzPerpetualCoreEnvironment.RETRY_FLAG.set(Boolean.FALSE);
                }

            } finally {
                if (env != null) {
                    env.destroy();
                }
            }
        });
    }
}
