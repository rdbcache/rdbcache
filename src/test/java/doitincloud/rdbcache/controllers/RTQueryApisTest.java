package doitincloud.rdbcache.controllers;

import com.google.common.io.CharStreams;
import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.commons.helpers.Utils;
import doitincloud.rdbcache.services.CacheOps;

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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

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
    private CacheOps cacheOps;

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

            Mockito.when(cacheOps.listAllTables()).thenReturn(testTable);
            PowerMockito.mockStatic(AppCtx.class);
            BDDMockito.when(AppCtx.getCacheOps()).thenReturn(cacheOps);

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

            //Mockito.when(cacheOps.listAllKeyInfos()).thenReturn(testKeys);
            //PowerMockito.mockStatic(AppCtx.class);
            //BDDMockito.when(AppCtx.getCacheOps()).thenReturn(cacheOps);

            // try different way
            //
            CacheOps cache = new CacheOps();
            cache.init();
            cache.handleEvent(null);
            //cache.handleApplicationReadyEvent(null);
            for (Map.Entry<String, Object> entry: testKeys.entrySet()) {
                cache.put(entry.getKey(), (Map<String, Object>) entry.getValue());
            }
            AppCtx.setCacheOps(cache);

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

            //CacheOps cacheOps = mock(CacheOps.class, Mockito.RETURNS_DEEP_STUBS);
            Mockito.when(cacheOps.listAllData(null)).thenReturn(testData);
            PowerMockito.mockStatic(AppCtx.class);
            BDDMockito.when(AppCtx.getCacheOps()).thenReturn(cacheOps);

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

            BDDMockito.when(AppCtx.getCacheOps()).thenReturn(cacheOps);

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