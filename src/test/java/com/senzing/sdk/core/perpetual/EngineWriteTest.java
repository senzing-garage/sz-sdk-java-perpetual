package com.senzing.sdk.core.perpetual;

import java.util.List;
import java.util.ArrayList;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import static org.junit.jupiter.api.TestInstance.Lifecycle;

import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzRecordKey;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.test.StandardTestDataLoader;
import com.senzing.sdk.test.SzEngineWriteTest;
import com.senzing.sdk.test.SzRecord;
import com.senzing.sdk.test.TestDataLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static com.senzing.sdk.SzFlag.*;

/**
 * Unit tests for {@link SzCoreDiagnostic}.
 */
@TestInstance(Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@TestMethodOrder(OrderAnnotation.class)
public class EngineWriteTest 
    extends AbstractPerpetualCoreTest 
    implements SzEngineWriteTest
{
    private TestData testData = new TestData();

    private SzPerpetualCoreEnvironment env = null;

    @Override
    public TestData getTestData() {
        return this.testData;
    }

    @Override
    public SzEngine getEngine() throws SzException {
        return this.env.getEngine();
    }

    @BeforeAll
    public void initializeEnvironment() {
        this.beginTests();
        this.initializeTestEnvironment();
        String settings = this.getRepoSettings();
        
        String instanceName = this.getClass().getSimpleName();
        
        this.env = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                             .instanceName(instanceName)
                                             .settings(settings)
                                             .verboseLogging(false)
                                             .concurrency(this.getConcurrency())
                                             .configRefreshPeriod(this.getConfigRefreshPeriod())
                                             .build();
    }

    /**
     * Overridden to configure some data sources.
     */
    protected void prepareRepository() {
        String instanceName = this.getInstanceName();
        String settings     = this.getRepoSettings();

        SzCoreEnvironment env = SzCoreEnvironment.newBuilder()
                                                 .instanceName(instanceName)
                                                 .settings(settings)
                                                 .verboseLogging(false)
                                                 .build();
        try {
            TestDataLoader loader = new StandardTestDataLoader(env);
        
            this.testData.loadData(loader);
        
        } finally {
            env.destroy();
        }
    }
    
    @AfterAll
    public void teardownEnvironment() {
        try {
            if (this.env != null) {
                this.env.destroy();
                this.env = null;
            }
            this.teardownTestEnvironment();
        } finally {
            this.endTests();
        }
    }

    public List<Arguments> getRecordPreviewDefaultArguments() {
        List<Arguments> baseArgs = this.getRecordPreviewArguments();

        List<Arguments> defaultArgs = new ArrayList<>(baseArgs.size());

        baseArgs.forEach(args -> {
            Object[] arr = args.get();

            if (arr[arr.length - 1] != null) return;
            defaultArgs.add(Arguments.of(arr[0]));
        });

        return defaultArgs;
    }

    @ParameterizedTest
    @MethodSource("getRecordPreviewDefaultArguments")
    @Order(150)
    public void testRecordPreviewDefaults(SzRecord record)
    {
        this.performTest(() -> {
            try {
                SzEngine engine = (SzEngine) this.env.getEngine();

                String defaultResult = engine.getRecordPreview(record.toString());

                String explicitResult = engine.getRecordPreview(
                    record.toString(), SZ_RECORD_PREVIEW_DEFAULT_FLAGS);
                    
                assertEquals(explicitResult, defaultResult,
                    "Explicitly setting default flags yields a different result "
                    + "than omitting the flags parameter to the SDK function.");
            
            } catch (Exception e) {
                fail("Unexpectedly failed getting entity by record", e);
            }
        });
    }    

    public List<Arguments> getAddRecordDefaultArguments() {
        List<Arguments> baseArgs = this.getAddRecordArguments();

        List<Arguments> defaultArgs = new ArrayList<>(baseArgs.size());

        baseArgs.forEach(args -> {
            Object[] arr = args.get();

            if (arr[arr.length - 1] != null) return;
            defaultArgs.add(Arguments.of(arr[0], arr[1]));
        });

        return defaultArgs;
    }

    @ParameterizedTest
    @MethodSource("getAddRecordDefaultArguments")
    @Order(250)
    public void testAddRecordDefaults(SzRecordKey recordKey, SzRecord record)
    {
        this.performTest(() -> {
            try {
                SzEngine engine = (SzEngine) this.env.getEngine();

                String defaultResult = engine.addRecord(recordKey, record.toString());

                String explicitResult = engine.addRecord(
                    recordKey, record.toString(), SZ_ADD_RECORD_DEFAULT_FLAGS);
                    
                assertEquals(explicitResult, defaultResult,
                    "Explicitly setting default flags yields a different result "
                    + "than omitting the flags parameter to the SDK function.");

            } catch (Exception e) {
                fail("Unexpectedly failed adding record", e);
            }
        });
    }    

    public List<Arguments> getReevaluateRecordDefaultArguments() {
        List<Arguments> baseArgs = this.getReevaluateRecordArguments();

        List<Arguments> defaultArgs = new ArrayList<>(baseArgs.size());

        baseArgs.forEach(args -> {
            Object[] arr = args.get();

            if (arr[arr.length - 1] != null) return;
            defaultArgs.add(Arguments.of(arr[0]));
        });

        return defaultArgs;
    }

    @ParameterizedTest
    @MethodSource("getReevaluateRecordDefaultArguments")
    @Order(450)
    public void testReevaluateRecordDefaults(SzRecordKey recordKey)
    {
        this.performTest(() -> {
            try {
                SzEngine engine = (SzEngine) this.env.getEngine();

                String defaultResult = engine.reevaluateRecord(recordKey);
                
                String explicitResult = engine.reevaluateRecord(
                    recordKey, SZ_REEVALUATE_RECORD_DEFAULT_FLAGS);
                    
                assertEquals(explicitResult, defaultResult,
                    "Explicitly setting default flags yields a different result "
                    + "than omitting the flags parameter to the SDK function.");

            } catch (Exception e) {
                fail("Unexpectedly failed reevaluating record", e);
            }
        });
    }    

    public List<Arguments> getReevaluateEntityDefaultArguments() {
        List<Arguments> baseArgs = this.getReevaluateEntityArguments();

        List<Arguments> defaultArgs = new ArrayList<>(baseArgs.size());

        baseArgs.forEach(args -> {
            Object[] arr = args.get();

            if (arr[arr.length - 1] != null) return;
            defaultArgs.add(Arguments.of(arr[0]));
        });

        return defaultArgs;
    }

    @ParameterizedTest
    @MethodSource("getReevaluateEntityDefaultArguments")
    @Order(550)
    public void testReevaluateEntityDefaults(long entityId)
    {
        this.performTest(() -> {
            try {
                SzEngine engine = (SzEngine) this.env.getEngine();

                String defaultResult = engine.reevaluateEntity(entityId);
                
                String explicitResult = engine.reevaluateEntity(
                    entityId, SZ_REEVALUATE_ENTITY_DEFAULT_FLAGS);
                    
                assertEquals(explicitResult, defaultResult,
                    "Explicitly setting default flags yields a different result "
                    + "than omitting the flags parameter to the SDK function.");
            
            } catch (Exception e) {
                fail("Unexpectedly failed reevaluating entity", e);
            }
        });
    }    

    public List<Arguments> getDeleteRecordDefaultArguments() {
        List<Arguments> baseArgs = this.getDeleteRecordArguments();

        List<Arguments> defaultArgs = new ArrayList<>(baseArgs.size());

        baseArgs.forEach(args -> {
            Object[] arr = args.get();

            if (arr[arr.length - 1] != null) return;
            defaultArgs.add(Arguments.of(arr[0]));
        });

        return defaultArgs;
    }

    @ParameterizedTest
    @MethodSource("getDeleteRecordDefaultArguments")
    @Order(650)
    public void testDeleteRecordDefaults(SzRecordKey recordKey)
    {
        this.performTest(() -> {
            try {
                SzEngine engine = (SzEngine) this.env.getEngine();

                String defaultResult = engine.deleteRecord(recordKey);
                
                String explicitResult = engine.deleteRecord(
                    recordKey, SZ_DELETE_RECORD_DEFAULT_FLAGS);
                    
                assertEquals(explicitResult, defaultResult,
                    "Explicitly setting default flags yields a different result "
                    + "than omitting the flags parameter to the SDK function.");

            } catch (Exception e) {
                fail("Unexpectedly failed deleting record", e);
            }
        });
    }
}
