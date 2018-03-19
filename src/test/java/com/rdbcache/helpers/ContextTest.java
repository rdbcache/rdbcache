package com.rdbcache.helpers;

import com.rdbcache.Application;
import com.rdbcache.configs.AppCtx;
import com.rdbcache.models.Monitor;
import com.rdbcache.models.StopWatch;
import com.rdbcache.repositories.MonitorRepo;

import com.rdbcache.repositories.SimpleMonitorRepo;
import com.rdbcache.repositories.SimpleStopWatchRepo;
import com.rdbcache.repositories.StopWatchRepo;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.mock.mockito.MockBean;

import org.mockito.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.argThat;

@RunWith(PowerMockRunner.class)
@PrepareForTest(AppCtx.class)
public class ContextTest {

    @Test
    public void constructorTest() {

        Context context = new Context();
        assertEquals("constructorTest", context.getAction());
        assertNotNull(context.getMonitor());
        assertNotNull(context.getTraceId());
    }

    @Test
    public void monitorStopWatchTest() {

        Context context = new Context();
        assertFalse(context.saveMonitorData());

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

        MonitorRepo monitorRepo = new SimpleMonitorRepo();

        StopWatchRepo stopWatchRepo = new SimpleStopWatchRepo();

        PowerMockito.mockStatic(AppCtx.class);

        BDDMockito.when(AppCtx.getVersionInfo()).thenReturn(new VersionInfo());

        BDDMockito.when(AppCtx.getMonitorRepo()).thenReturn(monitorRepo);

        BDDMockito.when(AppCtx.getStopWatchRepo()).thenReturn(stopWatchRepo);

        context.closeMonitor();
    }
}