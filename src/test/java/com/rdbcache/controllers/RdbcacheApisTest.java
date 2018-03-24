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

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@WebMvcTest(secure = false)
@ContextConfiguration(classes = {Configurations.class})
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

            Map<String, Object> data1 = (Map<String, Object>) map.get("data");
            assertTrue(data1.size() > 0);
            assertEquals("2", data1.get("id").toString());
            assertEquals("kevin@example.com", data1.get("email").toString());

            String key = (String) map.get("key");

            requestBuilder = MockMvcRequestBuilders.
                    get("/rdbcache/v1/get/" + key).
                    accept(MediaType.APPLICATION_JSON);

            actions = mockMvc.perform(requestBuilder);

            result = actions.andReturn();
            response = result.getResponse();

            assertEquals(200, response.getStatus());

            //System.out.println(body);

            map = Utils.toMap(body);

            assertTrue(map.containsKey("timestamp"));
            assertTrue(map.containsKey("duration"));
            assertTrue(map.containsKey("key"));
            assertTrue(map.containsKey("data"));
            assertTrue(map.containsKey("trace_id"));

            Map<String, Object> data2 = (Map<String, Object>) map.get("data");
            assertEquals(data1, data2);
            assertEquals(key, (String) map.get("key"));

            body = response.getContentAsString();


        } catch (Exception e) {
            e.printStackTrace();
            fail("caught an exception");
        }
    }

    @Test
    public void get_get2() {

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
}

