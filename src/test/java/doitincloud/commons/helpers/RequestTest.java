package doitincloud.commons.helpers;

import com.google.common.io.CharStreams;
import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.rdbcache.controllers.supports.Request;
import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.repositories.KeyInfoRepo;
import doitincloud.rdbcache.repositories.SimpleKeyInfoRepo;
import doitincloud.rdbcache.services.CacheOps;
import doitincloud.rdbcache.services.DbaseOps;

import doitincloud.rdbcache.supports.AnyKey;
import doitincloud.rdbcache.supports.Context;
import doitincloud.rdbcache.supports.KvPairs;
import org.junit.Before;
import org.junit.runner.RunWith;

import org.mockito.BDDMockito;
import org.mockito.Mockito;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.springframework.http.MediaType;
import org.springframework.mock.web.MockServletContext;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import javax.servlet.http.HttpServletRequest;

import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AppCtx.class})
public class RequestTest {

    private KeyInfoRepo keyInfoRepo;

    @Before
    public void setUp() throws Exception {

        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("test-keyinfos.json");
        assertNotNull(inputStream);
        String text = null;
        try (final Reader reader = new InputStreamReader(inputStream)) {
            text = CharStreams.toString(reader);
        }
        assertNotNull(text);
        Map<String, Object> map = Utils.toMap(text);
        assertNotNull(map);

        PowerMockito.mockStatic(AppCtx.class);

        keyInfoRepo = new SimpleKeyInfoRepo(map);
        BDDMockito.when(AppCtx.getKeyInfoRepo()).thenReturn(keyInfoRepo);

        DbaseOps dbaseOps = mock(DbaseOps.class, Mockito.RETURNS_DEEP_STUBS);
        assertNotNull(dbaseOps);

        Map<String, Object> tablesMap = new LinkedHashMap<>();
        tablesMap.put("user_table", "data");
        tablesMap.put("tb1", "data");
        Mockito.when(dbaseOps.getTablesMap(any(Context.class))).thenReturn(tablesMap);

        BDDMockito.when(AppCtx.getDbaseOps()).thenReturn(dbaseOps);

        CacheOps cacheOps = new CacheOps();
        cacheOps.init();
        cacheOps.handleEvent(null);

        BDDMockito.when(AppCtx.getCacheOps()).thenReturn(cacheOps);
    }

    protected HttpServletRequest getRequest(String api, String key, Object value, Optional<String> tableOpt,
                                            Optional<String> expireOpt, String queryString) {
        String url = "/rdbcache/v1/" + api;
        if (key != null) {
            url += "/" + key;
        }
        if (value != null) {
            url += "/" + value;
        }
        if (tableOpt != null && tableOpt.isPresent()) {
            url += "/" + tableOpt.get();
        }
        if (expireOpt != null && expireOpt.isPresent()) {
            url += "/" + expireOpt.get();
        }
        if (queryString != null) {
            url += "?" + queryString;
        }
        return MockMvcRequestBuilders.get(url).
                accept(MediaType.APPLICATION_JSON).
                buildRequest(new MockServletContext());
    }

    @Test
    public void process1() {

        try {
            String key = "*";
            Optional<String> tableOpt = Optional.of("tb1");
            Optional<String> expireOpt = Optional.of("30");
            String queryString = "id=1";

            HttpServletRequest request = getRequest("get", key, null, tableOpt, expireOpt, queryString);

            Context context = new Context();

            AnyKey anyKey = Request.process(context, request, new KvPairs(key), tableOpt, expireOpt);

            assertEquals(1, anyKey.size());
            KeyInfo keyInfo = anyKey.getKeyInfo();
            assertTrue(keyInfo.getIsNew());

            assertEquals("KeyInfo(true, tb1, 30, {id: {=: [1]}})", keyInfo.toString());

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getCause().getMessage());
        }
    }

    @Test
    public void process2() {

        try {
            String key = "01a089f3ab704c1aaecdbe13777538e0";

            HttpServletRequest request = getRequest("get", key, null, null, null, null);

            Context context = new Context();

            AnyKey anyKey = Request.process(context, request, new KvPairs(key), null, null);

            assertEquals(1, anyKey.size());
            KeyInfo keyInfo = anyKey.getKeyInfo();
            assertFalse(keyInfo.getIsNew());

            //System.out.println(keyInfo.toString());
            assertEquals("KeyInfo(false, user_table, 30, id = ?, [12466])", keyInfo.toString());

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getCause().getMessage());
        }
    }
}