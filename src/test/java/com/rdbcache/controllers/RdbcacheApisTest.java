package com.rdbcache.controllers;

import com.rdbcache.configs.Configurations;
import com.rdbcache.configs.PropCfg;
import com.rdbcache.helpers.Utils;
import com.rdbcache.repositories.DbaseRepo;
import com.rdbcache.services.LocalCache;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;

import org.springframework.mock.web.MockHttpServletResponse;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@WebMvcTest(secure = false)
@ContextConfiguration(classes = {Configurations.class, PropCfg.class})
public class RdbcacheApisTest {

    private MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new RdbcacheApis()).build();

    @Autowired
    LocalCache localCache;

    @Autowired
    DbaseRepo dbaseRepo;

    @Before
    public void setup() {
    }

    @Test
    public void get_get1() {

        try {
            RequestBuilder requestBuilder = MockMvcRequestBuilders.
                    get("/rdbcache/v1/get/*/user_table?id=2").
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
            assertTrue(map.containsKey("key"));
            assertTrue(map.containsKey("data"));
            assertTrue(map.containsKey("trace_id"));

            Map<String, Object> data = (Map<String, Object>) map.get("data");
            assertTrue(data.size() > 0);
            assertEquals("2", data.get("id").toString());
            assertEquals("kevin@example.com", data.get("email").toString());

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void get_get2() {

        try {
            RequestBuilder requestBuilder = MockMvcRequestBuilders.
                    get("/rdbcache/v1/get/my-test-hash-key/user_table?id=2").
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
            assertTrue(map.containsKey("key"));
            assertTrue(map.containsKey("data"));
            assertTrue(map.containsKey("trace_id"));

            Map<String, Object> data = (Map<String, Object>) map.get("data");
            assertTrue(data.size() > 0);
            assertEquals("2", data.get("id").toString());
            assertEquals("kevin@example.com", data.get("email").toString());

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void get_get3() {

        try {

            Map<String, Object> data1 = null, data2 = null;
            String key = null;

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/get/*/user_table?id=2").
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                Map<String, Object> map = Utils.toMap(body);

                key = (String) map.get("key");
                assertNotNull(key);

                data1 = (Map<String, Object>) map.get("data");
                assertNotNull(data1);
            }
            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/get/" + key).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                Map<String, Object> map = Utils.toMap(body);

                data2 = (Map<String, Object>) map.get("data");
                assertNotNull(data2);
            }

            assertEquals(data1, data2);

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void get_get4() {

        try {

            RequestBuilder requestBuilder = MockMvcRequestBuilders.
                    get("/rdbcache/v1/get/*/user_table?id=100").
                    accept(MediaType.APPLICATION_JSON);

            ResultActions actions = mockMvc.perform(requestBuilder);
            MvcResult result = actions.andReturn();
            MockHttpServletResponse response = result.getResponse();

            assertEquals(404, response.getStatus());

            requestBuilder = MockMvcRequestBuilders.
                    get("/rdbcache/v1/get/any_hash_key_not_existed").
                    accept(MediaType.APPLICATION_JSON);

            actions = mockMvc.perform(requestBuilder);
            result = actions.andReturn();
            response = result.getResponse();

            assertEquals(404, response.getStatus());

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void get_get5() {

        try {

            RequestBuilder requestBuilder = MockMvcRequestBuilders.
                    get("/rdbcache/v1/get/any_hash_key_not_existed").
                    accept(MediaType.APPLICATION_JSON);

            ResultActions actions = mockMvc.perform(requestBuilder);
            MvcResult result = actions.andReturn();
            MockHttpServletResponse response = result.getResponse();

            assertEquals(404, response.getStatus());

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void set_get1() {

        try {

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/set/test_hash_key/test_value").
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                Map<String, Object> map = Utils.toMap(body);

                String key = (String) map.get("key");
                assertNotNull(key);
                assertEquals("test_hash_key", key);
            }

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/get/test_hash_key").
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                //System.out.println(body);
                Map<String, Object> map = Utils.toMap(body);

                String value = (String) map.get("data");
                assertNotNull(value);
                assertEquals("test_value", value);
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void set_get2() {

        try {

            String json = "Hello World";

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/set/test_hash_key2/" + json).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
            }

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/get/test_hash_key2").
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                Map<String, Object> map = Utils.toMap(body);
                String data = (String) map.get("data");

                assertEquals(json, data);
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void set_post1() {

        try {

            String json = "{\"email\":\"test@example.com\",\"name\":\"Test\",\"dob\":\"1999-07-21\"}";

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        post("/rdbcache/v1/set/test_hash_key_post/user_table").
                        contentType(MediaType.APPLICATION_JSON).content(json).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
            }

            // allow time to synchronize data
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/get/test_hash_key_post").
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                System.out.println(body);
                Map<String, Object> map = Utils.toMap(body);
                Map<String, Object> data = (Map<String, Object>) map.get("data");
                assertNotNull(data.get("id"));
                data.remove("id");
                Map<String, Object> jsonMap = Utils.toMap(json);
                assertEquals(jsonMap, data);
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void set_post2() {

        try {

            String json = "{\"email\":\"test2@example.com\",\"name\":\"Test\",\"dob\":\"1999-07-21\"}";
            String key = null;
            Map<String, Object> data1 = null, data2 = null;

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        post("/rdbcache/v1/set/*/user_table/66").
                        contentType(MediaType.APPLICATION_JSON).content(json).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                Map<String, Object> map = Utils.toMap(body);

                key = (String) map.get("key");
                assertNotNull(key);
                assertNull(map.get("data"));

            }

            // allow time to synchronize data
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/get/" + key).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                //System.out.println(body);

                Map<String, Object> map = Utils.toMap(body);
                data2 = (Map<String, Object>) map.get("data");
                assertNotNull(data2.get("id"));
                data2.remove("id");

                data1 = Utils.toMap(json);
                assertEquals(data1, data2);
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void put_post1() {

        try {

            String json = "{\"name\":\"Test\"}";
            String key = null;

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        post("/rdbcache/v1/put/*/user_table/66?id=3").
                        contentType(MediaType.APPLICATION_JSON).content(json).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                Map<String, Object> map = Utils.toMap(body);

                key = (String) map.get("key");
                assertNotNull(key);
                assertNull(map.get("data"));

            }

            // allow time to synchronize data
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/get/" + key).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                //System.out.println(body);

                Map<String, Object> map = Utils.toMap(body);
                Map<String, Object> data = (Map<String, Object>) map.get("data");

                assertEquals("Test", (String) data.get("name"));
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void put_post2() {

        try {

            String json = "{\"email\":\"test3@example.com\",\"name\":\"Test3\",\"dob\":\"1999-07-21\"}";
            String key = null;
            Map<String, Object> data1 = null, data2 = null;

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        post("/rdbcache/v1/set/*/user_table/66").
                        contentType(MediaType.APPLICATION_JSON).content(json).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                Map<String, Object> map = Utils.toMap(body);

                key = (String) map.get("key");
                assertNotNull(key);
                assertNull(map.get("data"));

            }

            // allow time to synchronize data
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            {
                String update = "{\"name\":\"Test333\"}";

                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        put("/rdbcache/v1/put/" + key).
                        contentType(MediaType.APPLICATION_JSON).content(update).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
            }

            // allow time to synchronize data
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/get/" + key).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                //System.out.println(body);

                Map<String, Object> map = Utils.toMap(body);
                Map<String, Object> data = (Map<String, Object>) map.get("data");

                assertEquals("Test333", (String) data.get("name"));
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void getset_get1() {

        try {

            String value1 = "test value 001";
            String key = "test_hash_key";

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/getset/" + key + "/" + value1).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                //System.out.println(body);
                Map<String, Object> map = Utils.toMap(body);
                Map<String, Object> data = (Map<String, Object>) map.get("data");
                assertNotNull(data);
                assertEquals(0, data.size());

            }

            // allow time to synchronize data
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            String value2 = "test value 002";

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/getset/" + key + "/" + value2).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                //System.out.println(body);

                Map<String, Object> map = Utils.toMap(body);
                String value = (String) map.get("data");

                assertEquals(value1, value);
            }

            // allow time to synchronize data
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/get/" + key).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                //System.out.println(body);

                Map<String, Object> map = Utils.toMap(body);
                String value = (String) map.get("data");

                assertEquals(value2, value);
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void getset_post1() {

        try {

            String json = "{\"email\":\"test33@example.com\",\"name\":\"Test3\"}";
            String key = null;
            Map<String, Object> data1 = null, data2 = null;

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        post("/rdbcache/v1/getset/*/user_table?id=3").
                        contentType(MediaType.APPLICATION_JSON).content(json).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                System.out.println(body);
                Map<String, Object> map = Utils.toMap(body);
                key = (String) map.get("key");
                assertNotNull(key);
                data2 = (Map<String, Object>) map.get("data");
                assertNotNull(data2);
                data1 = Utils.toMap("{\"id\":3,\"email\":\"david@example.com\",\"name\":\"David C.\",\"dob\":\"1979-11-08\"}");
                assertEquals(data1, data2);

            }

            // allow time to synchronize data
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/get/" + key).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                System.out.println(body);

                Map<String, Object> map = Utils.toMap(body);
                data2 = (Map<String, Object>) map.get("data");
                assertNotNull(data2);
                assertEquals("test33@example.com", (String) data2.get("email"));
                assertEquals("Test3", (String) data2.get("name"));
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void getset_post2() {

        try {

            String json = "{\"email\":\"test5@example.com\",\"name\":\"Test5\"}";
            String key = null;
            Map<String, Object> data1 = null, data2 = null;

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        post("/rdbcache/v1/getset/*/user_table?id=5").
                        contentType(MediaType.APPLICATION_JSON).content(json).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                System.out.println(body);
                Map<String, Object> map = Utils.toMap(body);
                key = (String) map.get("key");
                assertNotNull(key);
                data2 = (Map<String, Object>) map.get("data");
                assertNotNull(data2);
                assertEquals(0, data2.size());
            }

            // allow time to synchronize data
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/get/" + key).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                System.out.println(body);

                Map<String, Object> map = Utils.toMap(body);
                data2 = (Map<String, Object>) map.get("data");
                assertNotNull(data2);
                assertEquals("test5@example.com", (String) data2.get("email"));
                assertEquals("Test5", (String) data2.get("name"));
                assertEquals("5", (String) data2.get("id").toString());
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void select_get1() {

        try {

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/select/user_table?limit=3").
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                //System.out.println(body);

                Map<String, Object> map = Utils.toMap(body);
                Map<String, Object>  data = (Map<String, Object>) map.get("data");
                assertNotNull(data);
                assertTrue(data.size() > 1);
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void select_get2() {

        try {

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/select/user_table?email=mike@example.com").
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                //System.out.println(body);

                Map<String, Object> map = Utils.toMap(body);
                Map<String, Object> data = (Map<String, Object>) map.get("data");
                assertNotNull(data);
                assertEquals(1, data.size());
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void select_get3() {

        try {

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        get("/rdbcache/v1/select/employees?emp_no=1").
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                //System.out.println(body);

                Map<String, Object> map = Utils.toMap(body);
                Map<String, Object> data = (Map<String, Object>) map.get("data");
                assertNotNull(data);
                assertEquals(0, data.size());
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void select_post_pull1() {

        try {

            String json = "[\"hash_key001\", \"hash_key002\", \"hash_key003\"]";
            Map<String, Object> data1 = null, data2 = null;

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        post("/rdbcache/v1/select/employees").
                        contentType(MediaType.APPLICATION_JSON).content(json).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                //System.out.println(body);

                Map<String, Object> map = Utils.toMap(body);
                data1 = (Map<String, Object>) map.get("data");
                assertNotNull(data1);
                assertEquals(3, data1.size());
            }

            // allow time to synchronize data
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        post("/rdbcache/v1/pull").
                        contentType(MediaType.APPLICATION_JSON).content(json).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                //System.out.println(body);

                Map<String, Object> map = Utils.toMap(body);
                data2 = (Map<String, Object>) map.get("data");
                assertNotNull(data2);
                assertEquals(3, data2.size());
                assertEquals(data1, data2);
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void select_post_push1() {

        try {

            String json = "[\"hash_key011\", \"hash_key012\", \"hash_key013\"]";
            Map<String, Object> data1 = null, data2 = null;

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        post("/rdbcache/v1/select/user_table").
                        contentType(MediaType.APPLICATION_JSON).content(json).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                System.out.println(body);

                Map<String, Object> map = Utils.toMap(body);
                data1 = (Map<String, Object>) map.get("data");
                assertNotNull(data1);
                assertEquals(3, data1.size());
            }
            /*
            // allow time to synchronize data
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            String batchUpdate = "{\"hash_key011\":{\"email\":\"mike.a@example.com\"},"+
                    "\"hash_key012\":{\"email\":\"kevin.b@example.com\"},"+
                    "\"hash_key013\":{\"email\":\"david.c@example.com\"}}";

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        post("/rdbcache/v1/push").
                        contentType(MediaType.APPLICATION_JSON).content(batchUpdate).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                System.out.println(body);

                Map<String, Object> map = Utils.toMap(body);
                List<String> keys = (List<String>) map.get("data");
                assertNotNull(keys);
                assertEquals(3, keys.size());
            }

            // allow time to synchronize data
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            {
                RequestBuilder requestBuilder = MockMvcRequestBuilders.
                        post("/rdbcache/v1/pull").
                        contentType(MediaType.APPLICATION_JSON).content(json).
                        accept(MediaType.APPLICATION_JSON);

                ResultActions actions = mockMvc.perform(requestBuilder);
                MvcResult result = actions.andReturn();
                MockHttpServletResponse response = result.getResponse();

                assertEquals(200, response.getStatus());
                String body = response.getContentAsString();
                System.out.println(body);

                Map<String, Object> map = Utils.toMap(body);
                data2 = (Map<String, Object>) map.get("data");
                assertNotNull(data2);
                assertEquals(3, data2.size());
            }
            */
        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

}

