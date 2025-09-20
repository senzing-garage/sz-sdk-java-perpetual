package com.senzing.sdk.core.perpetual;

import java.io.File;
import java.time.Duration;

import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.core.AbstractCoreTest;

/**
 * 
 */
public abstract class AbstractPerpetualCoreTest extends AbstractCoreTest 
{
    /**
     * Protected default constructor.
     */
    protected AbstractPerpetualCoreTest() {
        this(null);
    }

   /**
     * Protected constructor allowing the derived class to specify the
     * location for the entity respository.
     *
     * @param repoDirectory The directory in which to include the entity
     *                      repository.
     */
    protected AbstractPerpetualCoreTest(File repoDirectory) {
        super(repoDirectory);
    }

    /**
     * Gets the concurrency to use for this test suite.
     * This returns <code>null</code> by default.
     * 
     * @return The concurrency for this test suite.
     */
    protected Integer getConcurrency() {
        return null;
    }

    /**
     * Gets the configuration refresh period to use for this test suite.
     * This returns <code>null</code> by default.
     * 
     * @return The {@link Duration} for the configuration refresh period.
     */
    protected Duration getConfigRefreshPeriod() {
        return null;
    }

    /**
     * Creates a new default config and adds the specified zero or more
     * data sources to it and then returns the JSON {@link String} for that
     * config.
     * 
     * @param env The {@link SzEnvironment} to use.
     * 
     * @param dataSources The zero or more data sources to add to the config.
     * 
     * @return The JSON {@link String} that for the created config.
     */
    protected String createConfig(SzEnvironment env, String... dataSources) 
        throws SzException
    {
        SzConfig config = env.getConfigManager().createConfig();
        for (String dataSource : dataSources) {
            config.registerDataSource(dataSource);
        }
        return config.export();
    }

}
