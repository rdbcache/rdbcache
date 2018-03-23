package com.rdbcache.controllers;

import com.google.common.io.CharStreams;
import com.rdbcache.configs.AppCtx;
import com.rdbcache.configs.PropCfg;
import com.rdbcache.helpers.Utils;
import com.rdbcache.services.LocalCache;

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
import org.springframework.test.context.ContextConfiguration;
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

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(SpringRunner.class)
@WebMvcTest(value = {RTQueryApis.class}, secure = false)
@PrepareForTest(AppCtx.class)
public class RTQueryApisTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void retrieveVersionInfo() throws Exception {

        try {
            RequestBuilder requestBuilder = MockMvcRequestBuilders.
                    get("/rtquery/v1/version-info").
                    accept(MediaType.APPLICATION_JSON);

            ResultActions actions = mockMvc.perform(requestBuilder);
            MvcResult result = actions.andReturn();
            MockHttpServletResponse response = result.getResponse();

            assertEquals(200, response.getStatus());

            String body = response.getContentAsString();

            //System.out.println(body);

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
    }

    @Test
    public void retrieveConfigurations() throws Exception {

        RequestBuilder requestBuilder = MockMvcRequestBuilders.
                get("/rtquery/v1/configurations").
                accept(MediaType.APPLICATION_JSON);

        ResultActions actions = mockMvc.perform(requestBuilder);
        MvcResult result = actions.andReturn();
        MockHttpServletResponse response = result.getResponse();

        assertEquals(200, response.getStatus());

        String body = response.getContentAsString();

        //System.out.println(body);

        Map<String, Object> map = Utils.toMap(body);

        assertTrue(map.containsKey("timestamp"));
        assertTrue(map.containsKey("duration"));
        assertTrue(map.containsKey("data"));
        assertTrue(map.containsKey("trace_id"));

        Map<String, Object> data = (Map<String, Object>) map.get("data");
        assertTrue(data.size() > 0);
    }

    @Test
    public void retrieveProperties() throws Exception {

        RequestBuilder requestBuilder = MockMvcRequestBuilders.
                get("/rtquery/v1/properties").
                accept(MediaType.APPLICATION_JSON);

        ResultActions actions = mockMvc.perform(requestBuilder);
        MvcResult result = actions.andReturn();
        MockHttpServletResponse response = result.getResponse();

        assertEquals(200, response.getStatus());

        String body = response.getContentAsString();

        //System.out.println(body);

        Map<String, Object> map = Utils.toMap(body);

        assertTrue(map.containsKey("timestamp"));
        assertTrue(map.containsKey("duration"));
        assertTrue(map.containsKey("data"));
        assertTrue(map.containsKey("trace_id"));

        Map<String, Object> data = (Map<String, Object>) map.get("data");
        assertTrue(data.size() > 0);
    }

    @Test
    public void retrieveLocalCacheConfig() throws Exception {

        RequestBuilder requestBuilder = MockMvcRequestBuilders.
                get("/rtquery/v1/cache/config").
                accept(MediaType.APPLICATION_JSON);

        ResultActions actions = mockMvc.perform(requestBuilder);
        MvcResult result = actions.andReturn();
        MockHttpServletResponse response = result.getResponse();

        assertEquals(200, response.getStatus());

        String body = response.getContentAsString();

        //System.out.println(body);

        Map<String, Object> map = Utils.toMap(body);

        assertTrue(map.containsKey("timestamp"));
        assertTrue(map.containsKey("duration"));
        assertTrue(map.containsKey("data"));
        assertTrue(map.containsKey("trace_id"));

        Map<String, Object> data = (Map<String, Object>) map.get("data");
        assertTrue(data.size() > 0);

    }

    @MockBean
    private LocalCache localCache;

    @Test
    public void retrieveLocalCacheTable() throws Exception {

        try {
            InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("test-table.json");
            assertNotNull(inputStream);
            String text = null;
            try (final Reader reader = new InputStreamReader(inputStream)) {
                text = CharStreams.toString(reader);
            }
            assertNotNull(text);
            Map<String, Object> testTable = Utils.toMap(text);
            assertNotNull(testTable);

            Mockito.when(localCache.listAllTables()).thenReturn(testTable);
            PowerMockito.mockStatic(AppCtx.class);
            BDDMockito.when(AppCtx.getLocalCache()).thenReturn(localCache);

            RequestBuilder requestBuilder = MockMvcRequestBuilders.
                    get("/rtquery/v1/cache/table").
                    accept(MediaType.APPLICATION_JSON);

            ResultActions actions = mockMvc.perform(requestBuilder);
            MvcResult result = actions.andReturn();
            MockHttpServletResponse response = result.getResponse();

            assertEquals(200, response.getStatus());

            String body = response.getContentAsString();

            //System.out.println(body);

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
    }

    @Test
    public void retrieveLocalCacheKey() throws Exception {

        try {
            InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("test-keys.json");
            assertNotNull(inputStream);
            String text = null;
            try (final Reader reader = new InputStreamReader(inputStream)) {
                text = CharStreams.toString(reader);
            }
            assertNotNull(text);
            Map<String, Object> testKeys = Utils.toMap(text);
            assertNotNull(testKeys);

            //Mockito.when(localCache.listAllKeyInfos()).thenReturn(testKeys);
            //PowerMockito.mockStatic(AppCtx.class);
            //BDDMockito.when(AppCtx.getLocalCache()).thenReturn(localCache);

            // try different way
            //
            LocalCache cache = new LocalCache();
            cache.init();
            cache.handleEvent(null);
            //cache.handleApplicationReadyEvent(null);
            for (Map.Entry<String, Object> entry: testKeys.entrySet()) {
                cache.put(entry.getKey(), (Map<String, Object>) entry.getValue());
            }
            AppCtx.setLocalCache(cache);

            RequestBuilder requestBuilder = MockMvcRequestBuilders.
                    get("/rtquery/v1/cache/key").
                    accept(MediaType.APPLICATION_JSON);

            ResultActions actions = mockMvc.perform(requestBuilder);
            MvcResult result = actions.andReturn();
            MockHttpServletResponse response = result.getResponse();

            assertEquals(response.getStatus(), 200);

            String body = response.getContentAsString();

            //System.out.println(body);

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
    }

    @Test
    public void retrieveLocalCacheData() throws Exception {

        try {

            InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("test-data.json");

            assertNotNull(inputStream);
            String text = null;
            try (final Reader reader = new InputStreamReader(inputStream)) {
                text = CharStreams.toString(reader);
            }
            assertNotNull(text);
            Map<String, Object> testData = Utils.toMap(text);
            assertNotNull(testData);

            Mockito.when(localCache.listAllData()).thenReturn(testData);
            PowerMockito.mockStatic(AppCtx.class);
            BDDMockito.when(AppCtx.getLocalCache()).thenReturn(localCache);

            RequestBuilder requestBuilder = MockMvcRequestBuilders.
                    get("/rtquery/v1/cache/data").
                    accept(MediaType.APPLICATION_JSON);

            ResultActions actions = mockMvc.perform(requestBuilder);
            MvcResult result = actions.andReturn();
            MockHttpServletResponse response = result.getResponse();

            assertEquals(response.getStatus(), 200);

            String body = response.getContentAsString();

            //System.out.println(body);

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
    }

    @Test
    public void retrieveAppCtx() throws Exception {

        try {

            PowerMockito.mockStatic(AppCtx.class);

            BDDMockito.when(AppCtx.getLocalCache()).thenReturn(localCache);

            RequestBuilder requestBuilder = MockMvcRequestBuilders.
                    get("/rtquery/v1/app-ctx").
                    accept(MediaType.APPLICATION_JSON);

            ResultActions actions = mockMvc.perform(requestBuilder);
            MvcResult result = actions.andReturn();
            MockHttpServletResponse response = result.getResponse();

            assertEquals(response.getStatus(), 200);

            String body = response.getContentAsString();

            //System.out.println(body);

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
    }}