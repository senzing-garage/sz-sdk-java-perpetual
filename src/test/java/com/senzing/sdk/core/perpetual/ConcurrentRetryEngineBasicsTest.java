package com.senzing.sdk.core.perpetual;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.junit.jupiter.api.TestInstance.Lifecycle;

import java.time.Duration;

@TestInstance(Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class ConcurrentRetryEngineBasicsTest extends EngineBasicsTest
{
    private static final Integer CONCURRENCY = 4;


    private static final Duration DURATION = Duration.ofMillis(500);

    @Override
    protected Integer getConcurrency() {
        return CONCURRENCY;
    }

    @Override
    protected Duration getConfigRefreshPeriod() {
        return DURATION;
    }
}
