package com.rdbcache;

import com.rdbcache.helpers.Utils;
import com.rdbcache.queries.QueryInfo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class QueryTests {

    @BeforeAll
    static void initAll() {
    }

    @BeforeEach
    void init() {
    }

    @Test
    void succeedingTest() {
    }

    @Test
    public void queryTest001() {

//        String url = "http://localhost:8181/rdbcache/v1/get/*/test_table/10?id=a&id=b";
//
//        MockHttpServletRequest request = new MockHttpServletRequest();
//        request.se
//        UriComponents uriComponents = UriComponentsBuilder.fromHttpUrl(url).build();
//
//        System.out.println(Utils.toJson(uriComponents.getQueryParams()));
//
//        System.out.println(uriComponents.toUriString());
//
//        Map<String, String[]> params = uriComponents.getQueryParams().;
//        if (params != null && params.size() > 0) {
//            QueryInfo queryInfo = new QueryInfo(keyInfo.getTable(), params);
//            keyInfo.setQueryInfo(queryInfo);
//        }
    }

    @Test
    @Disabled("for demonstration purposes")
    void skippedTest() {
        // not executed
    }

    @AfterEach
    void tearDown() {
    }

    @AfterAll
    static void tearDownAll() {
    }

}
