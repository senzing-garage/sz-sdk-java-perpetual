package com.senzing.sdk.core.perpetual;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import com.senzing.sdk.SzException;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.test.StandardTestConfigurator;
import com.senzing.sdk.test.SzConfigManagerTest;
import com.senzing.sdk.core.SzCoreEnvironment;

import static org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import static org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@TestMethodOrder(OrderAnnotation.class)
public class ConfigManagerTest 
    extends AbstractPerpetualCoreTest 
    implements SzConfigManagerTest
{
    private SzPerpetualCoreEnvironment env = null;

    private TestData testData = new TestData();

    @Override
    public SzConfigManager getConfigManager() throws SzException {
        return this.env.getConfigManager();
    }

    @Override 
    public TestData getTestData() {
        return this.testData;
    }

    @BeforeAll
    public void initializeEnvironment() {
        this.beginTests();
        this.initializeTestEnvironment(true);

        String settings = this.getRepoSettings();
        
        String instanceName = this.getClass().getSimpleName();

        
        SzEnvironment env = SzCoreEnvironment.newBuilder()
                                             .instanceName(instanceName)
                                             .settings(settings)
                                             .verboseLogging(false)
                                             .build();

        try {
            StandardTestConfigurator configurator
                = new StandardTestConfigurator(env);

            this.testData.setup(configurator);

        } finally {
            env.destroy();
        }

        this.env = SzPerpetualCoreEnvironment.newPerpetualBuilder()
                                             .instanceName(instanceName)
                                             .settings(settings)
                                             .verboseLogging(false)
                                             .concurrency(this.getConcurrency())
                                             .configRefreshPeriod(this.getConfigRefreshPeriod())
                                             .build();
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
}
