package com.senzing.sdk.core.perpetual;

import java.util.List;
import java.util.ArrayList;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;


import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzFlag;
import com.senzing.sdk.SzRecordKey;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.test.StandardTestDataLoader;
import com.senzing.sdk.test.SzEngineReadTest;
import com.senzing.sdk.test.TestDataLoader;


import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static com.senzing.sdk.SzFlag.*;

/**
 * Unit tests for {@link SzCoreDiagnostic}.
 */
@TestInstance(Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class EngineReadTest 
    extends AbstractPerpetualCoreTest 
    implements SzEngineReadTest
{
    private SzPerpetualCoreEnvironment env = null;

    private TestData testData = new TestData();

    @Override
    public SzEngine getEngine() throws SzException {
        return this.env.getEngine();
    }

    @Override
    public TestData getTestData() {
        return this.testData;
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
     * Overridden to configure data sources and load test data.
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

    @ParameterizedTest
    @MethodSource("getRecordKeyParameters")
    public void testGetEntityByRecordIdDefaults(SzRecordKey recordKey)
    {
        this.performTest(() -> {
            try {
                SzEngine engine = (SzEngine) this.env.getEngine();

                String defaultResult = engine.getEntity(recordKey);
                
                String explicitResult = engine.getEntity(
                    recordKey, SZ_ENTITY_DEFAULT_FLAGS);
                
                assertEquals(explicitResult, defaultResult,
                    "Explicitly setting default flags yields a different result "
                    + "than omitting the flags parameter to the SDK function.");
            }
            catch (Exception e) {
                fail("Unexpectedly failed getting entity by record", e);
            }
        });
    }

    @ParameterizedTest
    @MethodSource("getRecordKeyParameters")
    public void testGetEntityByEntityIdDefaults(SzRecordKey recordKey)
    {
        this.performTest(() -> {
            try {
                SzEngine engine = (SzEngine) this.env.getEngine();

                long entityId = this.getEntityId(recordKey);

                String defaultResult = engine.getEntity(entityId);
                
                String explicitResult = engine.getEntity(
                    entityId, SZ_ENTITY_DEFAULT_FLAGS);
                    
                assertEquals(explicitResult, defaultResult,
                    "Explicitly setting default flags yields a different result "
                    + "than omitting the flags parameter to the SDK function.");
            }
            catch (Exception e) {
                fail("Unexpectedly failed getting entity by entity ID", e);
            }
        });
    }

    @ParameterizedTest
    @MethodSource("getRecordKeyParameters")
    public void testGetRecordDefaults(SzRecordKey recordKey) {
        this.performTest(() -> {
            try {
                SzEngine engine = (SzEngine) this.env.getEngine();

                String defaultResult = engine.getRecord(recordKey);
                
                String explicitResult = engine.getRecord(
                    recordKey, SZ_RECORD_DEFAULT_FLAGS);
                    
                assertEquals(explicitResult, defaultResult,
                    "Explicitly setting default flags yields a different result "
                    + "than omitting the flags parameter to the SDK function.");
            
            } catch (Exception e) {
                fail("Unexpectedly failed getting entity by record", e);
            }
        });
    }

    public List<Arguments> getSearchDefaultParameters()
    {
        List<Arguments> searchParams = this.getSearchParameters();

        List<Arguments> defaultParams = new ArrayList<>(searchParams.size());

        searchParams.forEach(args -> {
            Object[] arr = args.get();
            
            // skip parameters that expect exceptions
            if (arr[arr.length - 1] != null) return;

            defaultParams.add(Arguments.of(arr[0], arr[1]));
        });

        return defaultParams;
    }

    @ParameterizedTest
    @MethodSource("getSearchDefaultParameters")
    public void testSearchByAttributesdDefaults(String attributes,
                                                String searchProfile)
    {
        this.performTest(() -> {
            try {
                SzEngine engine = (SzEngine) this.env.getEngine();

                String defaultResult = null;
                try {
                    defaultResult = (searchProfile == null)
                        ? engine.searchByAttributes(attributes)
                        : engine.searchByAttributes(attributes, searchProfile);
                } catch (SzException e) {
                    throw new SzException(attributes, e);
                }

                String explicitResult = (searchProfile == null)
                    ? engine.searchByAttributes(attributes, 
                                                SzFlag.SZ_SEARCH_BY_ATTRIBUTES_DEFAULT_FLAGS)
                    : engine.searchByAttributes(attributes, 
                                                searchProfile,
                                                SzFlag.SZ_SEARCH_BY_ATTRIBUTES_DEFAULT_FLAGS);
                    
                assertEquals(explicitResult, defaultResult,
                    "Explicitly setting default flags yields a different result "
                    + "than omitting the flags parameter to the SDK function.");

            } catch (Exception e) {
                fail("Unexpectedly failed getting entity by record", e);
            }
        });
    }

}
