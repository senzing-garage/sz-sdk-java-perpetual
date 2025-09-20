package com.senzing.sdk.core.perpetual;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.senzing.io.IOUtilities;
import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzConfigRetryable;
import com.senzing.sdk.SzConfigurationException;
import com.senzing.sdk.SzDiagnostic;
import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzEntityIds;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.SzFlag;
import com.senzing.sdk.SzProduct;
import com.senzing.sdk.SzRecordKey;
import com.senzing.sdk.SzRecordKeys;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.sdk.test.StandardTestDataLoader;
import com.senzing.sdk.test.SzRecord;
import com.senzing.sdk.test.SzRecord.SzFullName;
import com.senzing.sdk.test.SzRecord.SzSocialSecurity;

import static org.junit.jupiter.api.TestInstance.Lifecycle;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.json.JsonArray;
import javax.json.JsonObject;

import static com.senzing.io.IOUtilities.UTF_8;
import static com.senzing.sdk.SzFlag.SZ_ADD_RECORD_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_DELETE_RECORD_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_ENTITY_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_EXPORT_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_FIND_NETWORK_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_FIND_PATH_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_HOW_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_RECORD_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_RECORD_PREVIEW_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_REDO_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_REEVALUATE_ENTITY_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_REEVALUATE_RECORD_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_SEARCH_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_VIRTUAL_ENTITY_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_WHY_ENTITIES_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_WHY_RECORDS_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_WHY_RECORD_IN_ENTITY_ALL_FLAGS;
import static com.senzing.sdk.SzFlag.SZ_WHY_SEARCH_ALL_FLAGS;
import static com.senzing.sdk.test.SdkTest.circularIterator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static com.senzing.util.JsonUtilities.*;

/**
 * Unit tests for {@link SzCoreDiagnostic}.
 */
@TestInstance(Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@TestMethodOrder(OrderAnnotation.class)
public class ConfigRetryTest extends AbstractPerpetualCoreTest 
{
    private static class MockEnvironment extends SzPerpetualCoreEnvironment {
        private static ThreadLocal<SzException> MOCK_FAILURE = new ThreadLocal<>();

        public MockEnvironment(String instanceName, String settings) {
            super(SzPerpetualCoreEnvironment.newPerpetualBuilder()
                    .settings(settings).instanceName(instanceName)
                    .configRefreshPeriod(REACTIVE_CONFIG_REFRESH));
        }

        public Object mock(SzException e) {
            MOCK_FAILURE.set(e);
            return null;
        }

        @Override
        protected <T> T doExecute(Callable<T> task) throws Exception
        {
            SzException mockFailure = MOCK_FAILURE.get();
            MOCK_FAILURE.set(null);

            if (mockFailure != null) {
                throw mockFailure;
            }
            return super.doExecute(task);
        }

    }
    /**
     * An empty object array.
     */
    private static final Object[] EMPTY_ARRAY = new Object[0];

    /**
     * A {@link Getter} that returns an empty array.
     */
    private static final Getter<Object[]> EMPTY_GETTER
        = (test, pre) -> EMPTY_ARRAY;

    /**
     * The data source code for the customers data source.
     */
    public static final String CUSTOMERS = "CUSTOMERS";

    /**
     * The data source code for the employees data source.
     */
    public static final String EMPLOYEES = "EMPLOYEES";

    /**
     * The record ID for record ABC123.
     */
    public static final String ABC123 = "ABC123";

    /**
     * The record ID for record DEF456.
     */
    public static final String DEF456 = "DEF456";

    /**
     * The {@link SzRecordKey} for customer ABC123.
     */
    public static final SzRecordKey CUSTOMER_ABC123
        = SzRecordKey.of(CUSTOMERS, ABC123);
    
    /**
     * The {@link SzRecordKey} for customer DEF456.
     */
    public static final SzRecordKey CUSTOMER_DEF456
        = SzRecordKey.of(CUSTOMERS, DEF456);

    /**
     * The {@link SzRecordKey} for employee ABC123.
     */
    public static final SzRecordKey EMPLOYEE_ABC123
        = SzRecordKey.of(EMPLOYEES, ABC123);
    
    /**
     * The {@link SzRecordKey} for employee DEF456.
     */
    public static final SzRecordKey EMPLOYEE_DEF456
        = SzRecordKey.of(EMPLOYEES, DEF456);
    
    /**
     * The record definition for the {@link #CUSTOMER_ABC123} 
     * and {@link #EMPLOYEE_ABC123} record keys.
     */
    public static final String RECORD_ABC123 = """
            {
                "NAME_FULL": "Joe Schmoe",
                "HOME_PHONE_NUMBER": "702-555-1212",
                "MOBILE_PHONE_NUMBER": "702-555-1313",
                "ADDR_FULL": "101 Main Street, Las Vegas, NV 89101"
            }
            """;
    
    /**
     * The record definition for the {@link #CUSTOMER_DEF456}
     * and {@link #EMPLOYEE_DEF456} record keys.
     */
    public static final String RECORD_DEF456 = """
            {
                "NAME_FULL": "Jane Schmoe",
                "HOME_PHONE_NUMBER": "702-555-1212",
                "MOBILE_PHONE_NUMBER": "702-555-1414",
                "ADDR_FULL": "101 Main Street, Las Vegas, NV 89101",
                "SSN_NUMBER": "888-88-8888"
            }
            """;
    
    /**
     * The {@link Set} of {@link SzRecord} instances to trigger
     * a redo so {@link SzEngine#processRedoRecord(String)} can be tested.
     */
    public static final Set<SzRecord> PROCESS_REDO_TRIGGER_RECORDS = Set.of(
        new SzRecord(
            SzRecordKey.of(EMPLOYEES, "SAME-SSN-11"),
            SzFullName.of("Anthony Stark"),
            SzSocialSecurity.of("888-88-8888")),
        new SzRecord(
            SzRecordKey.of(EMPLOYEES, "SAME-SSN-12"),
            SzFullName.of("Janet Van Dyne"),
            SzSocialSecurity.of("888-88-8888")),
        new SzRecord(
            SzRecordKey.of(EMPLOYEES, "SAME-SSN-13"),
            SzFullName.of("Henry Pym"),
            SzSocialSecurity.of("888-88-8888")),
        new SzRecord(
            SzRecordKey.of(EMPLOYEES, "SAME-SSN-14"),
            SzFullName.of("Bruce Banner"),
            SzSocialSecurity.of("888-88-8888")),
        new SzRecord(
            SzRecordKey.of(EMPLOYEES, "SAME-SSN-15"),
            SzFullName.of("Steven Rogers"),
            SzSocialSecurity.of("888-88-8888")),
        new SzRecord(
            SzRecordKey.of(EMPLOYEES, "SAME-SSN-16"),
            SzFullName.of("Clinton Barton"),
            SzSocialSecurity.of("888-88-8888")),
        new SzRecord(
            SzRecordKey.of(EMPLOYEES, "SAME-SSN-17"),
            SzFullName.of("Wanda Maximoff"),
            SzSocialSecurity.of("888-88-8888")),
        new SzRecord(
            SzRecordKey.of(EMPLOYEES, "SAME-SSN-18"),
            SzFullName.of("Victor Shade"),
            SzSocialSecurity.of("888-88-8888")),
        new SzRecord(
            SzRecordKey.of(EMPLOYEES, "SAME-SSN-19"),
            SzFullName.of("Natasha Romanoff"),
            SzSocialSecurity.of("888-88-8888")),
        new SzRecord(
            SzRecordKey.of(EMPLOYEES, "SAME-SSN-20"),
            SzFullName.of("James Rhodes"),
            SzSocialSecurity.of("888-88-8888")));

    /**
     * A fake redo record for attempting redo pre-reinitialize.
     */
    public static final Iterator<String> FAKE_REDO_RECORDS;
    
    static {
        List<String> list = new LinkedList<>();
        for (SzRecord record : PROCESS_REDO_TRIGGER_RECORDS) {
            SzRecordKey recordKey = record.getRecordKey();
            list.add("""
                {
                    "REASON": "LibFeatID[?] of FTypeID[7] went generic for CANDIDATES",
                    "DATA_SOURCE": "<DATA_SOURCE>",
                    "RECORD_ID": "<RECORD_ID>",
                    "REEVAL_ITERATION": 1,
                    "ENTITY_CORRUPTION_TRANSIENT": false,
                    "DSRC_ACTION": "X"
                }
                """.replaceAll("<DATA_SOURCE>", recordKey.dataSourceCode())
                   .replaceAll("<RECORD_ID>", recordKey.recordId()));
        }
        FAKE_REDO_RECORDS = circularIterator(list);
    }

    /**
     * The environment for this instance.
     */
    private MockEnvironment env = null;

    /**
     * The {@link Set} of feature ID's to use for the tests.
     */
    private Set<Long> featureIds = null;

    /**
     * The {@link Map} if {@link SzRecordKey} keys to {@link Long}
     * entity ID values.
     */
    private Map<SzRecordKey, Long> byRecordKeyLookup = null;

    /**
     * The {@link ServerSocket} with which to communicate to the sub-process.
     */
    private ServerSocket serverSocket = null;
    
    /**
     * The socket for communicating with the sub-process.
     */
    private Socket socket = null;

    /**
     * The {@link ObjectInputStream} for communicating
     * with the sub-process.
     */
    private ObjectInputStream objInputStream = null;

    /**
     * The {@link ObjectOutputStream} for communicating
     * with the sub-process.
     */
    private ObjectOutputStream objOutputStream = null;

    /**
     * The iteration count to keep the datasources changing.
     */
    private int iteration = -1;

    /**
     * Gets the entity ID for the specified {@link SzRecordKey}.
     * 
     * @param key The {@link SzRecordKey} for which to lookup the entity.
     * 
     * @return The entity ID for the specified {@link SzRecordKey}, or
     *         <code>null</code> if not found.
     */
    private Long getEntityId(SzRecordKey key) {
        return this.byRecordKeyLookup.get(key);
    }

    /**
     * Increments the iteration and returns the incremented value.
     * 
     * @return The incremented iteration value.
     */
    private synchronized int incrementIteration() {
        return (++this.iteration);
    }

    /**
     * Gets the current iteration.
     * 
     * @return The current iteration.
     */
    private synchronized int getIteration() {
        return this.iteration;
    }

    /**
     * Creates and returns an {@link SzRecordKey} for the specified
     * record ID and the current data source iteration.
     * 
     * @return The {@link SzRecordKey} for the specified record ID
     *         and the current data source iteration.
     */
    private synchronized SzRecordKey getRecordKey(String recordId) {
        String dataSource = CUSTOMERS + "-" + this.getIteration();
        return SzRecordKey.of(dataSource, recordId);
    }

    @BeforeAll
    public void initializeEnvironment() {
        this.beginTests();
        this.initializeTestEnvironment();
        String settings = this.getRepoSettings();
        String instanceName = this.getClass().getSimpleName();

        this.env = new MockEnvironment(instanceName, settings);

        try {
            SzEngine engine = this.env.getEngine();
            engine.addRecord(EMPLOYEE_ABC123, RECORD_ABC123);
            engine.addRecord(EMPLOYEE_DEF456, RECORD_DEF456);
            for (SzRecord record : PROCESS_REDO_TRIGGER_RECORDS) {
                engine.addRecord(record.getRecordKey(), record.toString());
            }

        } catch (SzException e) {
            fail("Failed to load record", e);
        }
 
        // change config and load data in a sub-process
        this.executeSubProcess();
    }

    /**
     * Overridden to configure <b>ONLY</b> the {@link #EMPLOYEES} data source.
     * 
     * @param excludeConfig 
     */
    protected void prepareRepository() {
        String settings     = this.getRepoSettings();
        String instanceName = this.getInstanceName();

        SzEnvironment env = SzCoreEnvironment.newBuilder()
                                             .instanceName(instanceName)
                                             .settings(settings)
                                             .build();

        try {
            StandardTestDataLoader loader = new StandardTestDataLoader(env);
            loader.configureDataSources(EMPLOYEES);
        } finally {
            env.destroy();
        }
    }

    /**
     * Executes a sub-process that will add a data source to the config
     * and load two records to that data source and then wait for that
     * process to commplete.
     */
    private void executeSubProcess() {
        try {
            // setup the server socket
            this.serverSocket = new ServerSocket(0, 20, InetAddress.getLoopbackAddress());

            File repoDirectory = this.getRepositoryDirectory();
            File initFile = new File(repoDirectory, "sz-init.json");
            
            String buildDirProp = System.getProperty("project.build.directory");
            File buildDir       = new File(buildDirProp);
            File wrapperDir     = new File(buildDir, "java-wrapper");
            File binDir         = new File(wrapperDir, "bin");
            File wrapper        = new File(binDir, "java-wrapper.bat");

            String[] cmdArray = new String[] {
                wrapper.getCanonicalPath(),
                "-cp",
                System.getProperty("java.class.path"),
                "-Xmx1000M",
                "-Dproject.build.directory=" + buildDirProp,
                ConfigRetryTest.class.getName(),
                initFile.getCanonicalPath(),
                String.valueOf(this.serverSocket.getLocalPort())
            };

            Runtime runtime = Runtime.getRuntime();

            Process process = runtime.exec(cmdArray);

            Thread.sleep(5000);

            if (!process.isAlive()) {
                int exitCode = process.exitValue();
                fail("Failed to launch alternate process to update config: " + exitCode);
            }

            this.socket = this.serverSocket.accept();
            this.objOutputStream = new ObjectOutputStream(this.socket.getOutputStream());
            this.objInputStream = new ObjectInputStream(this.socket.getInputStream());

        } catch (InterruptedException|IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void processNetwork(String network) {
        try {
            JsonObject  jsonObj = parseJsonObject(network);
            JsonArray   jsonArr = getJsonArray(jsonObj, "ENTITIES");

            Map<SzRecordKey, Long>      byRecordKeyMap  = new LinkedHashMap<>();
            Set<Long>                   featureIds      = new LinkedHashSet<>();

            for (JsonObject entityObj : jsonArr.getValuesAs(JsonObject.class)) {
                entityObj = getJsonObject(entityObj, "RESOLVED_ENTITY");
                
                // get the feature ID's
                JsonObject features = getJsonObject(entityObj, "FEATURES");
                features.values().forEach((jsonVal) -> {
                    JsonArray featureArr = (JsonArray) jsonVal;
                    for (JsonObject featureObj : featureArr.getValuesAs(JsonObject.class)) {
                        Long featureId = getLong(featureObj, "LIB_FEAT_ID");
                        featureIds.add(featureId);
                    }
                });

                // get the entity ID
                Long entityId = getLong(entityObj, "ENTITY_ID");

                // get the record keys
                JsonArray recordArr = getJsonArray(entityObj, "RECORDS");
                for (JsonObject recordObj : recordArr.getValuesAs(JsonObject.class)) {
                    String dataSource   = getString(recordObj, "DATA_SOURCE");
                    String recordId     = getString(recordObj, "RECORD_ID");

                    if (CUSTOMERS.equals(dataSource)) {
                        SzRecordKey recordKey = SzRecordKey.of(dataSource, recordId);
                        byRecordKeyMap.put(recordKey, entityId);
                    }
                }
            }

            this.featureIds         = Collections.unmodifiableSet(featureIds);
            this.byRecordKeyLookup  = Collections.unmodifiableMap(byRecordKeyMap);

        } catch (Exception e) {
            fail("Failed to parse entity network: " + network, e);
        }
    }

    @AfterAll
    public void teardownEnvironment() {
        try {
            try {
                if (this.objOutputStream != null) {
                    this.objOutputStream.writeObject(null);
                    this.objOutputStream.flush();
                    try {
                        Thread.sleep(2000L);
                    } catch (InterruptedException ignore) {
                        // do nothing
                    }
                    this.objOutputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (this.objInputStream != null) {
                    this.objInputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (this.socket != null) {
                    this.socket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (this.serverSocket != null) {
                    this.serverSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (this.env != null) {
                this.env.destroy();
                this.env = null;
            }
            this.teardownTestEnvironment();
        } finally {
            this.endTests();
        }
    }

    public interface Getter<T> {
        T get(ConfigRetryTest test, Object pre) throws SzException;
    }

    public interface PreProcess {
        Object process(ConfigRetryTest test)
            throws SzException;
    }

    public interface PostProcess {
        void process(ConfigRetryTest test, Object pre, Object result)
            throws SzException;
    }

    private static Object[] arrayOf(Object... elems) {
        return elems;
    }

    private static void addMethod(Set<Method>       handledMethods,
                                  List<Arguments>   results,
                                  Getter<?>         getter,
                                  Method            method, 
                                  Boolean           expectRetryable,
                                  Getter<Object[]>  paramGetter)
    {
        addMethod(handledMethods, results, getter, method, expectRetryable, paramGetter, null, null);
    }

    private static void addMethod(Set<Method>       handledMethods,
                                  List<Arguments>   results,
                                  Getter<?>         getter,
                                  Method            method, 
                                  Boolean           expectRetryable,
                                  Getter<Object[]>  paramGetter,
                                  PreProcess        preProcess,
                                  PostProcess       postProcess)
                                  
    {
        if (handledMethods.contains(method)) return;
        results.add(Arguments.of(getter, method, expectRetryable, paramGetter, preProcess, postProcess));
        handledMethods.add(method);
    }

    private static void addProductMethods(Set<Method>       handledMethods,
                                          List<Arguments>   results)
    {
        try {
            addMethod(handledMethods, 
                     results, 
                     (test, pre) -> test.env.getProduct(),
                     SzProduct.class.getMethod("getVersion"),
                     false,
                     EMPTY_GETTER);

            addMethod(handledMethods, 
                     results, 
                     (test, pre) -> test.env.getProduct(),
                     SzProduct.class.getMethod("getLicense"),
                     false,
                     EMPTY_GETTER);

            // handle any methods not explicitly handled
            Method[] methods = SzProduct.class.getMethods();
            for (Method method : methods) {
                addMethod(handledMethods, results, 
                          (test, pre) -> null,
                          method,
                          null,
                          null);
            }

        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static void addConfigManagerMethods(Set<Method>     handledMethods,
                                                List<Arguments> results)
    {
        try {
            // handle config manager methods
            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getConfigManager(), 
                      SzConfigManager.class.getMethod("createConfig"),
                      Boolean.FALSE,
                      EMPTY_GETTER);

            addMethod(handledMethods,
                      results, 
                      (test, pre) -> test.env.getConfigManager(),
                      SzConfigManager.class.getMethod("createConfig", Long.TYPE),
                      Boolean.FALSE,
                      (test, pre) -> arrayOf(test.env.getConfigManager().getDefaultConfigId()));

            addMethod(handledMethods,
                      results, 
                      (test, pre) -> test.env.getConfigManager(),
                      SzConfigManager.class.getMethod("createConfig", String.class),
                      Boolean.FALSE,
                      (test, pre) -> arrayOf(test.env.getConfigManager().createConfig().export()));

            addMethod(handledMethods,
                      results, 
                      (test, pre) -> test.env.getConfigManager(),
                      SzConfigManager.class.getMethod(
                        "registerConfig", String.class, String.class),
                      Boolean.FALSE,
                      (test, pre) -> arrayOf(test.env.getConfigManager().createConfig().export(), "Template Config"));

            addMethod(handledMethods,
                      results, 
                      (test, pre) -> test.env.getConfigManager(),
                      SzConfigManager.class.getMethod(
                        "registerConfig", String.class),
                      Boolean.FALSE,
                      (test, pre) -> {
                        SzConfig config = test.env.getConfigManager().createConfig();
                        config.registerDataSource(EMPLOYEES);
                        return arrayOf(config.export());
                      });
                
            addMethod(handledMethods,
                      results, 
                      (test, pre) -> test.env.getConfigManager(),
                      SzConfigManager.class.getMethod("getConfigRegistry"),
                      Boolean.FALSE,
                      EMPTY_GETTER);

            addMethod(handledMethods,
                      results, 
                      (test, pre) -> test.env.getConfigManager(),
                      SzConfigManager.class.getMethod("getDefaultConfigId"),
                      Boolean.FALSE,
                      EMPTY_GETTER);
            
            addMethod(handledMethods,
                      results, 
                      (test, pre) -> test.env.getConfigManager(),
                      SzConfigManager.class.getMethod("replaceDefaultConfigId", Long.TYPE, Long.TYPE),
                      Boolean.FALSE,
                      null);

            addMethod(handledMethods,
                      results, 
                      (test, pre) -> test.env.getConfigManager(),
                      SzConfigManager.class.getMethod("setDefaultConfigId", Long.TYPE),
                      Boolean.FALSE,
                      null);
            
            addMethod(handledMethods,
                      results, 
                      (test, pre) -> test.env.getConfigManager(),
                      SzConfigManager.class.getMethod("setDefaultConfig", String.class, String.class),
                      Boolean.FALSE,
                      null);

            addMethod(handledMethods,
                      results, 
                      (test, pre) -> test.env.getConfigManager(),
                      SzConfigManager.class.getMethod("setDefaultConfig", String.class),
                      Boolean.FALSE,
                      null);

            Method[] methods = SzConfigManager.class.getMethods();
            for (Method method : methods) {
                addMethod(handledMethods, 
                          results,
                          (test, pre) -> null,
                          method,
                          null,
                          null);
            }

        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

    }

    private static void addConfigMethods(Set<Method>     handledMethods,
                                         List<Arguments> results)
    {
        try {
            // handle config methods
            addMethod(handledMethods, 
                      results,
                      (test, pre) -> test.env.getConfigManager().createConfig(),
                      SzConfig.class.getMethod("export"),
                      Boolean.FALSE,
                      EMPTY_GETTER);
            
            addMethod(handledMethods, 
                      results,
                      (test, pre) -> test.env.getConfigManager().createConfig(),
                      SzConfig.class.getMethod("getDataSourceRegistry"),
                      Boolean.FALSE,
                      EMPTY_GETTER);
            
            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getConfigManager().createConfig(),
                      SzConfig.class.getMethod("registerDataSource", String.class),
                      Boolean.FALSE,
                      (test, pre) -> arrayOf(CUSTOMERS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getConfigManager().createConfig(
                        test.env.getConfigManager().getDefaultConfigId()),
                      SzConfig.class.getMethod("unregisterDataSource", String.class),
                      Boolean.FALSE,
                      (test, pre) -> arrayOf(CUSTOMERS));

            Method[] methods = SzConfig.class.getMethods();
            for (Method method : methods) {
                addMethod(handledMethods, 
                          results,
                          (test, pre) -> null,
                          method,
                          null,
                          null);
            }

        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

    }

    private static void addDiagnosticMethods(Set<Method>        handledMethods,
                                             List<Arguments>    results)
    {
        try {
            // handle config methods
            addMethod(handledMethods, 
                      results,
                      (test, pre) -> test.env.getDiagnostic(),
                      SzDiagnostic.class.getMethod("getRepositoryInfo"),
                      Boolean.FALSE,
                      EMPTY_GETTER);
            
            addMethod(handledMethods, 
                      results,
                      (test, pre) -> test.env.getDiagnostic(),
                      SzDiagnostic.class.getMethod("checkRepositoryPerformance", Integer.TYPE),
                      Boolean.FALSE,
                      (test, pre) -> arrayOf(5));
            
            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getDiagnostic(),
                      SzDiagnostic.class.getMethod("purgeRepository"),
                      Boolean.FALSE,
                      null);

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getDiagnostic(),
                      SzDiagnostic.class.getMethod("getFeature", Long.TYPE),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.featureIds.iterator().next()),
                      (test) -> test.env.mock(new SzConfigurationException("mock")),
                      null);

            Method[] methods = SzDiagnostic.class.getMethods();
            for (Method method : methods) {
                addMethod(handledMethods, 
                          results,
                          (test, pre) -> null,
                          method,
                          null,
                          null);
            }

        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

    }

    private static void addEngineMethods(Set<Method>        handledMethods,
                                         List<Arguments>    results)
    {
        try {
            // handle config methods
            addMethod(handledMethods, 
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("primeEngine"),
                      Boolean.FALSE,
                      EMPTY_GETTER);
            
            addMethod(handledMethods, 
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("getStats"),
                      Boolean.FALSE,
                      EMPTY_GETTER);
            
            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("getRecordPreview", String.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(RECORD_ABC123, SZ_RECORD_PREVIEW_ALL_FLAGS),
                      (test) -> test.env.mock(new SzConfigurationException("mock")),
                      null);

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("getRecordPreview", String.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(RECORD_ABC123),
                      (test) -> test.env.mock(new SzConfigurationException("mock")),
                      null);

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("searchByAttributes", String.class, String.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(RECORD_ABC123, null, SZ_SEARCH_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("searchByAttributes", String.class, String.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(RECORD_ABC123, null));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("searchByAttributes", String.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(RECORD_ABC123, SZ_SEARCH_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("searchByAttributes", String.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(RECORD_ABC123));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("whySearch", String.class, Long.TYPE, String.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(RECORD_ABC123, test.getEntityId(CUSTOMER_DEF456), null, SZ_WHY_SEARCH_ALL_FLAGS),
                      (test) -> test.env.mock(new SzConfigurationException("mock")),
                      null);
                      

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("whySearch", String.class, Long.TYPE, String.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(RECORD_ABC123, test.getEntityId(CUSTOMER_DEF456), null),
                      (test) -> test.env.mock(new SzConfigurationException("mock")),
                      null);
            
            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("whySearch", String.class, Long.TYPE, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(RECORD_ABC123, test.getEntityId(CUSTOMER_DEF456), SZ_WHY_SEARCH_ALL_FLAGS),
                      (test) -> test.env.mock(new SzConfigurationException("mock")),
                      null);

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("whySearch", String.class, Long.TYPE),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(RECORD_ABC123, test.getEntityId(CUSTOMER_DEF456)),
                      (test) -> test.env.mock(new SzConfigurationException("mock")),
                      null);

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("getEntity", Long.TYPE, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getEntityId(CUSTOMER_ABC123), SZ_ENTITY_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("getEntity", Long.TYPE),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getEntityId(CUSTOMER_ABC123)));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("getEntity", SzRecordKey.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(CUSTOMER_ABC123, SZ_ENTITY_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("getEntity", SzRecordKey.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(CUSTOMER_ABC123));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("findInterestingEntities", Long.TYPE, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getEntityId(CUSTOMER_ABC123), SZ_ENTITY_ALL_FLAGS),
                      (test) -> test.env.mock(new SzConfigurationException("mock")),
                      null);

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("findInterestingEntities", Long.TYPE),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getEntityId(CUSTOMER_ABC123)),
                      (test) -> test.env.mock(new SzConfigurationException("mock")),
                      null);

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("findInterestingEntities", SzRecordKey.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getRecordKey(ABC123), SZ_ENTITY_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("findInterestingEntities", SzRecordKey.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getRecordKey(ABC123)));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "findPath", Long.TYPE, Long.TYPE, Integer.TYPE, SzEntityIds.class, Set.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getEntityId(CUSTOMER_ABC123),
                                        test.getEntityId(CUSTOMER_DEF456),
                                        3,
                                        null,
                                        null,
                                        SZ_FIND_PATH_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "findPath", Long.TYPE, Long.TYPE, Integer.TYPE, SzEntityIds.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getEntityId(CUSTOMER_ABC123),
                                        test.getEntityId(CUSTOMER_DEF456),
                                        3,
                                        null,
                                        null));
                      
            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "findPath", Long.TYPE, Long.TYPE, Integer.TYPE, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getEntityId(CUSTOMER_ABC123),
                                        test.getEntityId(CUSTOMER_DEF456),
                                        3,
                                        SZ_FIND_PATH_ALL_FLAGS));
            
            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "findPath", Long.TYPE, Long.TYPE, Integer.TYPE),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getEntityId(CUSTOMER_ABC123),
                                        test.getEntityId(CUSTOMER_DEF456),
                                        3));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "findPath", SzRecordKey.class, SzRecordKey.class, Integer.TYPE, SzRecordKeys.class, Set.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(CUSTOMER_ABC123,
                                        CUSTOMER_DEF456,
                                        3,
                                        null,
                                        null,
                                        SZ_FIND_PATH_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "findPath", SzRecordKey.class, SzRecordKey.class, Integer.TYPE, SzRecordKeys.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(CUSTOMER_ABC123,
                                        CUSTOMER_DEF456,
                                        3,
                                        null,
                                        null));
                      
            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "findPath", SzRecordKey.class, SzRecordKey.class, Integer.TYPE, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(CUSTOMER_ABC123,
                                        CUSTOMER_DEF456,
                                        3,
                                        SZ_FIND_PATH_ALL_FLAGS));
            
            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "findPath", SzRecordKey.class, SzRecordKey.class, Integer.TYPE),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(CUSTOMER_ABC123, CUSTOMER_DEF456, 3));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "findNetwork", SzEntityIds.class, Integer.TYPE, Integer.TYPE, Integer.TYPE, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(SzEntityIds.of(test.getEntityId(CUSTOMER_ABC123), 
                                                       test.getEntityId(CUSTOMER_DEF456)),
                                        3, 0, 10, SZ_FIND_NETWORK_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "findNetwork", SzEntityIds.class, Integer.TYPE, Integer.TYPE, Integer.TYPE),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(SzEntityIds.of(test.getEntityId(CUSTOMER_ABC123), 
                                                       test.getEntityId(CUSTOMER_DEF456)),
                                        3, 0, 10));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "findNetwork", SzRecordKeys.class, Integer.TYPE, Integer.TYPE, Integer.TYPE, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(SzRecordKeys.of(CUSTOMER_ABC123, CUSTOMER_DEF456),
                                        3, 0, 10, SZ_FIND_NETWORK_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "findNetwork", SzRecordKeys.class, Integer.TYPE, Integer.TYPE, Integer.TYPE),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(SzRecordKeys.of(CUSTOMER_ABC123, CUSTOMER_DEF456), 3, 0, 10));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "whyRecordInEntity", SzRecordKey.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getRecordKey(ABC123), SZ_WHY_RECORD_IN_ENTITY_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "whyRecordInEntity", SzRecordKey.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getRecordKey(ABC123)));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "whyRecords", SzRecordKey.class, SzRecordKey.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getRecordKey(ABC123), CUSTOMER_DEF456, SZ_WHY_RECORDS_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "whyRecords", SzRecordKey.class, SzRecordKey.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getRecordKey(ABC123), CUSTOMER_DEF456));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "whyEntities", Long.TYPE, Long.TYPE, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getEntityId(CUSTOMER_ABC123), 
                                        test.getEntityId(CUSTOMER_DEF456), 
                                        SZ_WHY_ENTITIES_ALL_FLAGS),
                      (test) -> test.env.mock(new SzConfigurationException("mock")),
                      null);

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "whyEntities", Long.TYPE, Long.TYPE),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getEntityId(CUSTOMER_ABC123), 
                                        test.getEntityId(CUSTOMER_DEF456)),
                      (test) -> test.env.mock(new SzConfigurationException("mock")),
                      null);

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod(
                        "howEntity", Long.TYPE, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getEntityId(CUSTOMER_ABC123), 
                                             SZ_HOW_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("howEntity", Long.TYPE),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getEntityId(CUSTOMER_ABC123)));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("getVirtualEntity", Set.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(SzRecordKeys.of(test.getRecordKey(ABC123), CUSTOMER_DEF456), 
                                     SZ_VIRTUAL_ENTITY_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("getVirtualEntity", Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(SzRecordKeys.of(test.getRecordKey(ABC123), CUSTOMER_DEF456)));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("getRecord", SzRecordKey.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getRecordKey(ABC123), SZ_RECORD_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("getRecord", SzRecordKey.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getRecordKey(ABC123)));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("exportJsonEntityReport", Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(SZ_EXPORT_ALL_FLAGS),
                      null,
                      (test, pre, handle) -> { 
                        if (handle != null) { test.env.getEngine().closeExportReport((Long) handle); }});

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("exportJsonEntityReport"),
                      Boolean.TRUE,
                      EMPTY_GETTER,
                      null,
                      (test, pre, handle) -> { 
                        if (handle != null) { test.env.getEngine().closeExportReport((Long) handle); }});

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("exportCsvEntityReport", String.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf("*", SZ_EXPORT_ALL_FLAGS),
                      null,
                      (test, pre, handle) -> { 
                        if (handle != null) { test.env.getEngine().closeExportReport((Long) handle); }});

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("exportCsvEntityReport", String.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf("*"),
                      null,
                      (test, pre, handle) -> { 
                        if (handle != null) { test.env.getEngine().closeExportReport((Long) handle); }});

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("closeExportReport", Long.TYPE),
                      Boolean.FALSE,
                      null); // requires a valid export handle which cannot be gotten

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("fetchNext", Long.TYPE),
                      Boolean.TRUE,
                      null); // requires a valid export handle which cannot be gotten

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("countRedoRecords"),
                      Boolean.FALSE,
                      EMPTY_GETTER);

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("getRedoRecord"),
                      Boolean.TRUE,
                      EMPTY_GETTER,
                      (test) -> test.env.mock(new SzConfigurationException("mock")),
                      null);

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("processRedoRecord", String.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(FAKE_REDO_RECORDS.next(), SZ_REDO_ALL_FLAGS),
                      (test) -> test.env.mock(new SzConfigurationException("mock")),
                      null);

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("processRedoRecord", String.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(FAKE_REDO_RECORDS.next()),
                      (test) -> test.env.mock(new SzConfigurationException("mock")),
                      null);

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("addRecord", SzRecordKey.class, String.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) ->
                        arrayOf(test.getRecordKey(ABC123), RECORD_ABC123, SZ_ADD_RECORD_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("addRecord", SzRecordKey.class, String.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getRecordKey(DEF456), RECORD_DEF456));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("reevaluateRecord", SzRecordKey.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getRecordKey(ABC123), SZ_REEVALUATE_RECORD_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("reevaluateRecord", SzRecordKey.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getRecordKey(DEF456)));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("reevaluateEntity", Long.TYPE, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getEntityId(CUSTOMER_ABC123), SZ_REEVALUATE_ENTITY_ALL_FLAGS),
                      (test) -> test.env.mock(new SzConfigurationException("mock")),
                      null);


            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("reevaluateEntity", Long.TYPE),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getEntityId(CUSTOMER_DEF456)),
                      (test) -> test.env.mock(new SzConfigurationException("mock")),
                      null);


            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("deleteRecord", SzRecordKey.class, Set.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getRecordKey(ABC123), SZ_DELETE_RECORD_ALL_FLAGS));

            addMethod(handledMethods,
                      results,
                      (test, pre) -> test.env.getEngine(),
                      SzEngine.class.getMethod("deleteRecord", SzRecordKey.class),
                      Boolean.TRUE,
                      (test, pre) -> arrayOf(test.getRecordKey(DEF456)));

            Method[] methods = SzEngine.class.getMethods();
            for (Method method : methods) {
                addMethod(handledMethods, 
                          results,
                          (test, pre) -> null,
                          method,
                          null,
                          null);
            }

        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

    }

    public List<Arguments> getTestParameters() {
        List<Arguments> results = new LinkedList<>();

        Set<Method> handledMethods = new LinkedHashSet<>();

        addProductMethods(handledMethods, results);

        addConfigManagerMethods(handledMethods, results);

        addConfigMethods(handledMethods, results);

        addDiagnosticMethods(handledMethods, results);

        addEngineMethods(handledMethods, results);

        // handle the diagnostic methods
        return results;
    }

    @ParameterizedTest
    @MethodSource("getTestParameters")
    public void testConfigRetryMethod(Getter<?>         getter,
                                      Method            method,
                                      Boolean           expectRetryable,
                                      Getter<Object[]>  paramGetter,
                                      PreProcess        preProcess,
                                      PostProcess       postProcess) 
    {
        this.performTest(() -> {
            try {
                SzConfigRetryable retryable = method.getAnnotation(SzConfigRetryable.class);
                if (expectRetryable == null) {
                    fail("Method from interface (" + method.getDeclaringClass().getSimpleName()
                        + ") is " + (retryable == null ? "not " : "") 
                        + "annotated and has no explicit config retryable test defined: "
                        + method.toString());
                }
                
                assertEquals(expectRetryable, (retryable != null),
                            "Method from interface (" + method.getDeclaringClass().getSimpleName() 
                            + ") is " + ((retryable == null) ? "not " : "") + "retryable but should "
                            + ((retryable == null) ? "not " : "") + "be: " + method.toString());
                
                if (paramGetter == null) return;

                // force the configs to be out of sync
                this.objOutputStream.writeObject(this.incrementIteration());
                String network = (String) this.objInputStream.readObject();
                this.processNetwork(network);

                Object preProcessResult = (preProcess == null) ? null : preProcess.process(this);
                
                Object target = getter.get(this, preProcessResult);
                
                Object[] params = paramGetter.get(this, preProcessResult);

                Object result = null;
                int retriedCount1 = this.env.getRetriedCount();
                int failedCount1  = this.env.getRetriedFailureCount();
                int retriedCount2 = retriedCount1;
                int failedCount2  = failedCount1;
                try {
                    // this may or may not succeed
                    result = method.invoke(target, params);

                } catch (InvocationTargetException e) {
                    fail((expectRetryable ? "Annotated" : "Non-annotated") + " method from " 
                        + method.getDeclaringClass().getSimpleName()
                        + " got an exception: " + method.toString(), e);

                } finally {
                    retriedCount2 = this.env.getRetriedCount();
                    failedCount2  = this.env.getRetriedFailureCount();
                    if (postProcess != null) {
                        postProcess.process(this, preProcessResult, result);
                    }
                }

                if (expectRetryable) {
                    assertEquals(1, (retriedCount2-retriedCount1),
                                 "Number of retried operations (" + retriedCount1 + " vs " + retriedCount2 
                                 + ") did NOT increase by expected amount for annotated method from " 
                                 + method.getDeclaringClass().getSimpleName() + ": " + method.toString());
                                 
                } else {
                    assertEquals(0, (retriedCount2-retriedCount1),
                                 "Number of retried operations (" + retriedCount1 + " vs " + retriedCount2 
                                 + ") unexpectedly changed for annotated method from " 
                                 + method.getDeclaringClass().getSimpleName() + ": " + method.toString());
                }

                assertEquals(0, (failedCount2-failedCount1),
                                "Number of failed retried operations (" + failedCount1 + " vs "
                                + failedCount2 + ") unexpectedly changed for "
                                + (expectRetryable ? "annotated" : "non-annoated") + " method from "
                                + method.getDeclaringClass().getSimpleName() + ": " + method.toString());
                
            } catch (Exception e) {
                fail("Method from " + method.getDeclaringClass().getSimpleName() 
                    + " failed with exception: " + method.toString(), e);
            }
        });
    }

    /**
     * Provides the main for the second process that changes the 
     * config and loads records.  The path to the repository's
     * initialization file is the expected command-line argument
     * and the path to the output file.
     * 
     * 
     * @param args The command-line arguments.
     */
    public static void main(String[] args) {
        SzEnvironment env = null;
        try {
            if (args.length < 2) {
                System.err.println("Must specify the following command-line arguments:");
                System.err.println("  1: Path to setting JSON file for the repository");
                System.err.println("  2: The port to connect to");
                System.exit(1);
            }

            String  initFilePath    = args[0];
            int     port            = 0;
            File    initFile        = new File(initFilePath);
            
            try {
                port = Integer.parseInt(args[1]);
            } catch (Exception e) {
                System.err.println("The specifid port number is not an integer: " + args[1]);
                System.exit(1);
            }

            if (!initFile.exists()) {
                System.err.println("Settings file does not exist: " + initFilePath);
                System.exit(1);
            }

            String initJson = IOUtilities.readTextFileAsString(initFile, UTF_8);

            env = SzCoreEnvironment.newBuilder()
                                   .settings(initJson)
                                   .instanceName("Alternate SzConfigRetryable")
                                   .build();

            SzConfigManager configMgr = env.getConfigManager();

            try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), port);
                 ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                 ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream()))
            {
                for (Integer iteration = (Integer) ois.readObject(); 
                     iteration != null; 
                     iteration = (Integer) ois.readObject()) 
                {
                    String dataSource = CUSTOMERS;
                    if (iteration > 0) {
                        dataSource = dataSource + "-" + iteration;
                    }
                    SzConfig config = configMgr.createConfig(configMgr.getDefaultConfigId());
                    config.registerDataSource(dataSource);
                    long configId = configMgr.setDefaultConfig(config.export());
                    env.reinitialize(configId);

                    SzEngine engine = env.getEngine();

                    SzRecordKey recordKey1 = SzRecordKey.of(dataSource, EMPLOYEE_ABC123.recordId());
                    SzRecordKey recordKey2 = SzRecordKey.of(dataSource, EMPLOYEE_DEF456.recordId());

                    engine.addRecord(recordKey1, RECORD_ABC123);
                    engine.addRecord(recordKey2, RECORD_DEF456);

                    String network = engine.findNetwork(
                        SzRecordKeys.of(recordKey1, recordKey2),
                                        2, 0, 0,
                                        EnumSet.allOf(SzFlag.class));
                    
                    
                    oos.writeObject(network);
                    oos.flush();
                }
            }

        } catch (Exception e) {
            String  buildDirProp    = System.getProperty("project.build.directory");
            File    buildDir        = new File(buildDirProp);
            String  className       = ConfigRetryTest.class.getSimpleName();
            String  outFileName     = className + "-errors.txt";
            File    outFile         = new File(buildDir, outFileName);
            try (FileOutputStream   fos = new FileOutputStream(outFile);
                 OutputStreamWriter osw = new OutputStreamWriter(fos, UTF_8);
                 PrintWriter        pw  = new PrintWriter(osw))
            {
                for (int index = 0; index < args.length; index++) {
                    pw.println("ARG " + index + ": " + args[index]);
                }
                pw.println();
                e.printStackTrace(pw);
                pw.flush();

            } catch (IOException e2) {
                e2.printStackTrace();
            }

            e.printStackTrace();
            System.exit(1);

        } finally {
            if (env != null) {
                env.destroy();
            }
        }
    }
}
