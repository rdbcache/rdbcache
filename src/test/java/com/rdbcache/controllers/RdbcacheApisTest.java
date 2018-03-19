package com.rdbcache.controllers;

import com.google.common.io.CharStreams;
import com.rdbcache.configs.AppCtx;
import com.rdbcache.helpers.AnyKey;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.KvPairs;
import com.rdbcache.helpers.Utils;
import com.rdbcache.models.*;
import com.rdbcache.repositories.*;
import com.rdbcache.services.AsyncOps;
import com.rdbcache.services.DbaseOps;
import com.rdbcache.services.LocalCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.booleanThat;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(SpringRunner.class)
@WebMvcTest(value = RdbcacheApis.class, secure = false)
@PrepareForTest(AppCtx.class)
public class RdbcacheApisTest {

    @Autowired
    private MockMvc mockMvc;

    private LocalCache localCache;

    private AsyncOps asyncOps;

    private DbaseOps dbaseOps;

    private Map<String, Object> testData;

    private Map<String, Object> testKeys;

    private Map<String, Object> testTable;

    @Before
    public void setUp() throws Exception {

        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("test-table.json");
        assertNotNull(inputStream);
        String text = null;
        try (final Reader reader = new InputStreamReader(inputStream)) {
            text = CharStreams.toString(reader);
        }
        assertNotNull(text);
        testTable = Utils.toMap(text);

        inputStream = this.getClass().getClassLoader().getResourceAsStream("test-keys.json");
        assertNotNull(inputStream);
        text = null;
        try (final Reader reader = new InputStreamReader(inputStream)) {
            text = CharStreams.toString(reader);
        }
        assertNotNull(text);
        testKeys = Utils.toMap(text);

        inputStream = this.getClass().getClassLoader().getResourceAsStream("test-data.json");
        assertNotNull(inputStream);
        text = null;
        try (final Reader reader = new InputStreamReader(inputStream)) {
            text = CharStreams.toString(reader);
        }
        assertNotNull(text);
        testData = Utils.toMap(text);

        localCache = new LocalCache();
        localCache.init();
        localCache.handleEvent(null);
        //localCache.handleApplicationReadyEvent(null);
        for (Map.Entry<String, Object> entry: testTable.entrySet()) {
            localCache.put(entry.getKey(), (Map<String, Object>) entry.getValue());
        }
        //AppCtx.setLocalCache(localCache);

        asyncOps = new AsyncOps();
        asyncOps.init();
        asyncOps.handleEvent(null);
        //asyncOps.handleApplicationReadyEvent(null);
        //AppCtx.setAsyncOps(asyncOps);

        dbaseOps = new DbaseOps();
        dbaseOps.init();
        dbaseOps.handleEvent(null);
        //dbaseOps.handleApplicationReadyEvent(null);
        //AppCtx.setDbaseOps(dbaseOps);

        PowerMockito.mockStatic(AppCtx.class);

        BDDMockito.when(AppCtx.getLocalCache()).thenReturn(localCache);

        BDDMockito.when(AppCtx.getAsyncOps()).thenReturn(asyncOps);

        BDDMockito.when(AppCtx.getDbaseOps()).thenReturn(dbaseOps);

        BDDMockito.when(AppCtx.getDbaseRepo()).thenReturn(new SimpleDbaseRepo(testData));

        BDDMockito.when(AppCtx.getKeyInfoRepo()).thenReturn(new SimpleKeyInfoRepo(testKeys));

        BDDMockito.when(AppCtx.getRedisRepo()).thenReturn(new SimpleRedisRepo(testData));

        BDDMockito.when(AppCtx.getKvPairRepo()).thenReturn(new SimpleKvPairRepo());

        BDDMockito.when(AppCtx.getMonitorRepo()).thenReturn(new SimpleMonitorRepo());

        BDDMockito.when(AppCtx.getStopWatchRepo()).thenReturn(new SimpleStopWatchRepo());
    }

    @After
    public void tearDown() throws Exception {
        //localCache.interrupt();
    }

    @Test
    public void get_get() {
    /*
        try {

            RequestBuilder requestBuilder = MockMvcRequestBuilders.
                    get("/rdbcache/v1/get/01a089f3ab704c1aaecdbe13777538e0").
                    accept(MediaType.APPLICATION_JSON);

            ResultActions actions = mockMvc.perform(requestBuilder);
            MvcResult result = actions.andReturn();
            MockHttpServletResponse response = result.getResponse();

            assertEquals(200, response.getStatus());

            String body = response.getContentAsString();

            System.out.println(body);

            Map<String, Object> map = Utils.toMap(body);

            assertTrue(map.containsKey("timestamp"));
            assertTrue(map.containsKey("duration"));
            assertTrue(map.containsKey("data"));
            assertTrue(map.containsKey("trace_id"));

            Map<String, Object> data = (Map<String, Object>) map.get("data");
            assertTrue(data.size() > 0);

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getCause().getMessage());
        }
        */
    }

    @Test
    public void set_get() {
    }

    @Test
    public void set_post() {
    }

    @Test
    public void put_post() {
    }

    @Test
    public void getset_get() {
    }

    @Test
    public void getset_post() {
    }

    @Test
    public void pull_post() {
    }

    @Test
    public void push_post() {
    }

    @Test
    public void delkey_get() {
    }

    @Test
    public void delkey_post() {
    }

    @Test
    public void delall_get() {
    }

    @Test
    public void delall_post() {
    }

    @Test
    public void select_get() {
    }

    @Test
    public void select_post() {
    }

    @Test
    public void save_post() {
    }

    @Test
    public void trace_get() {
    }

    @Test
    public void trace_post() {
    }

    @Test
    public void flushcache_get() {
    }
}