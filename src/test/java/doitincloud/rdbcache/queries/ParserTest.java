package doitincloud.rdbcache.queries;

import com.google.common.io.CharStreams;
import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.commons.helpers.Context;
import doitincloud.commons.helpers.Utils;
import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.models.KvPair;
import doitincloud.rdbcache.services.DbaseOps;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ParserTest {

    @Test
    public void prepareConditions() {

        QueryInfo queryInfo1 = new QueryInfo("user_table");
        Map<String, String[]> params = new LinkedHashMap<>();
        params.put("limit", new String[]{"2"});
        params.put("id", new String[]{"1", "2", "3"});

        Parser.prepareConditions(queryInfo1, params);
        //System.out.println(Utils.toJsonMap(queryInfo1));
        String json = "{\"table\":\"user_table\",\"conditions\":{\"id\":{\"=\":[\"1\",\"2\",\"3\"]}},\"limit\":2}";
        Map<String, Object> map2 = Utils.toMap(json);
        QueryInfo queryInfo2 = Utils.toPojo(map2, QueryInfo.class);

        assertEquals(queryInfo1, queryInfo2);
        assertEquals(queryInfo1.getKey(), queryInfo2.getKey());
    }

    @Test
    public void prepareQueryClauseParams() {

        Context context = new Context();
        KvPair pair = new KvPair("hash_key");
        KeyInfo keyInfo = new KeyInfo();
        keyInfo.setExpire("100");
        keyInfo.setTable("user_table");
        String json = "{\"table\":\"user_table\",\"conditions\":{\"id\":{\"=\":[\"1\",\"2\",\"3\"]}},\"limit\":2}";
        Map<String, Object> map = Utils.toMap(json);
        QueryInfo queryInfo = Utils.toPojo(map, QueryInfo.class);

        keyInfo.setQuery(queryInfo);

        Parser.prepareQueryClauseParams(context, pair, keyInfo);

        assertEquals("(id = ? OR id = ? OR id = ?)", keyInfo.getClause());
        assertEquals("[1, 2, 3]", keyInfo.getParams().toString());
    }

    @Test
    public void prepareStandardClauseParams() {

        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("test-table.json");
        assertNotNull(inputStream);
        String text = null;
        try (final Reader reader = new InputStreamReader(inputStream)) {
            text = CharStreams.toString(reader);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getCause().getMessage());
        }
        assertNotNull(text);
        Map<String, Object> testTable = Utils.toMap(text);
        assertNotNull(testTable);

        DbaseOps dbaseOps = new DbaseOps();
        AppCtx.setDbaseOps(dbaseOps);

        Context context = new Context();

        String json = "{\n" +
                "    \"id\" : 12467,\n" +
                "    \"email\" : \"kevin@example.com\",\n" +
                "    \"name\" : \"Kevin B.\",\n" +
                "    \"dob\" : \"1980-07-21\"\n" +
                "  }";

        KvPair pair = new KvPair("*", "data", Utils.toMap(json));

        try {
            KeyInfo keyInfo = new KeyInfo();
            keyInfo.setExpire("100");
            keyInfo.setTable("user_table");
            keyInfo.setCreatedAt(1522367710621L);

            String json2 = "{\"table\":\"user_table\",\"conditions\":{\"id\":{\"=\":[\"1\",\"2\",\"3\"]}},\"limit\":2}";
            QueryInfo queryInfo = Utils.toPojo(Utils.toMap(json2), QueryInfo.class);

            keyInfo.setQuery(queryInfo);
            keyInfo.setColumns((Map<String, Object>) testTable.get("table_columns::user_table"));
            keyInfo.setPrimaryIndexes(Arrays.asList("id"));

            Parser.prepareStandardClauseParams(context, pair, keyInfo);
            //System.out.println(Utils.toJsonMap(keyInfo));
            assertEquals("{\"expire\":\"100\",\"table\":\"user_table\",\"clause\":\"id = ?\"," +
                            "\"params\":[12467],\"query\":{\"table\":\"user_table\",\"conditions\":{\"id\":{\"=\":" +
                            "[\"1\",\"2\",\"3\"]}},\"limit\":2},\"query_key\":\"87677684c30a46c6e5afec88d0131410\"," +
                            "\"is_new\":false,\"expire_old\":\"180\",\"created_at\":1522367710621}",
                    Utils.toJsonMap(keyInfo));
            keyInfo.cleanup();
            //System.out.println(Utils.toJsonMap(keyInfo));
            assertEquals("{\"expire\":\"100\",\"table\":\"user_table\",\"clause\":\"id = ?\",\"params\":[12467]" +
                            ",\"query_key\":\"87677684c30a46c6e5afec88d0131410\",\"is_new\":false,\"created_at\":1522367710621}",
                    Utils.toJsonMap(keyInfo));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getCause().getMessage());
        }
    }
}