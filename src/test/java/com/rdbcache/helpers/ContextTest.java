package com.rdbcache.helpers;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.models.StopWatch;
import com.rdbcache.models.Monitor;
import com.rdbcache.repositories.MonitorRepo;
import com.rdbcache.repositories.StopWatchRepo;

import com.rdbcache.services.DbaseOps;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.mock.mockito.MockBean;

import org.mockito.*;
import org.springframework.context.annotation.Bean;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AppCtx.class})
public class ContextTest {

    @Test
    public void constructorTest() {

        Context context = new Context();
        assertEquals("constructorTest", context.getAction());
        assertNotNull(context.getMonitor());
        assertNotNull(context.getTraceId());
    }

    @Test
    public void monitorTest() {

        try {
            Context context = new Context();
            assertFalse(context.isMonitorEnabled());
            StopWatch watch = context.startStopWatch("testType", "test0");
            assertNull(watch);

            context.enableMonitor("test", "testType", "testAction");

            watch = context.startStopWatch("testType", "test1");
            assertNotNull(watch);
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
            }
            watch.stopNow();

            Long duration = context.stopFirstStopWatch();
            assertNotNull(duration);

            watch = context.startStopWatch("testType", "test2");
            assertNotNull(watch);
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
            }
            watch.stopNow();

            PowerMockito.mockStatic(AppCtx.class);

            BDDMockito.when(AppCtx.getVersionInfo()).thenReturn(new VersionInfo());

            DbaseOps dbaseOps = mock(DbaseOps.class, Mockito.RETURNS_DEEP_STUBS);
            BDDMockito.when(AppCtx.getDbaseOps()).thenReturn(dbaseOps);
            Mockito.when(dbaseOps.saveMonitor(context)).thenReturn(true);

            MonitorRepo monitorRepo = mock(MonitorRepo.class, Mockito.RETURNS_DEEP_STUBS);
            BDDMockito.when(AppCtx.getMonitorRepo()).thenReturn(monitorRepo);
            Mockito.when(monitorRepo.save(any(Monitor.class))).thenAnswer(invocation -> anyVararg());

            StopWatchRepo stopWatchRepo = mock(StopWatchRepo.class, Mockito.RETURNS_DEEP_STUBS);
            BDDMockito.when(AppCtx.getStopWatchRepo()).thenReturn(stopWatchRepo);
            Mockito.when(stopWatchRepo.save(any(StopWatch.class))).thenAnswer(invocation -> anyVararg());

            assertEquals(3, context.getMonitor().getStopWatches().size());

            context.closeMonitor();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getCause().getMessage());
        }
    }
}