package com.rdbcache.controllers;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.helpers.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

@RestController
@Profile({"dev", "test", "prod"})
public class TestInspectApis {

    /**
     * query version info
     *
     * @param request HttpServletRequest
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/query-version-info",
    }, method = RequestMethod.GET)
    public ResponseEntity<?> queryVerionInfo(
            HttpServletRequest request) {

        Context context = new Context(false);
        Request.process(context, request);

        VersionInfo versionInfo = new VersionInfo();

        Map<String, Object> data = Utils.getObjectMapper().convertValue(versionInfo, Map.class);

        return Response.send(context, data);
    }

    /**
     * query configurations
     *
     * @param request HttpServletRequest
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/query-configurations",
            "/v1/query-configurations/{nameOpt}",
    }, method = RequestMethod.GET)
    public ResponseEntity<?> queryConfigurations(
            HttpServletRequest request,
            @PathVariable Optional<String> nameOpt) {

        Context context = new Context(false);
        Request.process(context, request);

        Map<String, Object> data = Utils.toMap(PropCfg.printConfigurations());

        if (nameOpt.isPresent()) {
            Map<String, Object> map = new LinkedHashMap<String, Object>();
            String key = nameOpt.get();
            map.put(key, data.get(key));
            data = map;
        }

        return Response.send(context, data);
    }

    @Autowired
    Environment env;

    /**
     * query properties
     *
     * @param request HttpServletRequest
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/query-properties",
            "/v1/query-properties/{nameOpt}",
    }, method = RequestMethod.GET)
    public ResponseEntity<?> queryProperties(
            HttpServletRequest request,
            @PathVariable Optional<String> nameOpt) {

        Context context = new Context(false);
        Request.process(context, request);

        Map<String, Object> data = new LinkedHashMap<String, Object>();
        for(Iterator it = ((AbstractEnvironment) env).getPropertySources().iterator(); it.hasNext(); ) {
            PropertySource propertySource = (PropertySource) it.next();
            if (propertySource instanceof MapPropertySource) {
                data.putAll(((MapPropertySource) propertySource).getSource());
            }
        }

        if (nameOpt.isPresent()) {
            Map<String, Object> map = new LinkedHashMap<String, Object>();
            String key = nameOpt.get();
            map.put(key, data.get(key));
            data = map;
        }

        return Response.send(context, data);
    }

    /**
     * query cache
     *
     * query local cache
     *
     * @param request HttpServletRequest
     * @param action config, table, key and data
     * @param keyOpt, optional for specific key
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/query-cache/{action}",
            "/v1/query-cache/{action}/{keyOpt}"
    }, method = RequestMethod.GET)
    public ResponseEntity<?> queryCache(
            HttpServletRequest request,
            @PathVariable String action,
            @PathVariable Optional<String> keyOpt) {

        Context context = new Context(false);
        Request.process(context, request);

        String key = null;
        if (keyOpt.isPresent()) key = keyOpt.get();

        Map<String, Object> data = new LinkedHashMap<>();
        if (action.equals("config")) {
            if (key == null || key.equals("keyMinCacheTTL")) {
                data.put("keyMinCacheTTL", PropCfg.getKeyMinCacheTTL());
            }
            if (key == null || key.equals("dataMaxCacheTLL")) {
                data.put("dataMaxCacheTLL", PropCfg.getDataMaxCacheTLL());
            }
            if (key == null || key.equals("tableInfoCacheTTL")) {
                data.put("tableInfoCacheTTL", PropCfg.getTableInfoCacheTTL());
            }
        } else if (action.equals("table")) {
            if (key == null) {
                AppCtx.getLocalCache().listAllTables(data);
            } else {
                AppCtx.getLocalCache().getTable(key, data);
            }
        } else if (action.equals("key")) {
            if (key == null) {
                AppCtx.getLocalCache().listAllKeyInfos(data);
            } else {
                AppCtx.getLocalCache().getKeyInfo(key, data);
            }
        } else if (action.equals("data")) {
            if (key == null) {
                AppCtx.getLocalCache().listAllData(data);
            } else {
                AppCtx.getLocalCache().getData(key, data);
            }
        }

        return Response.send(context, data);
    }

    private static List<String> ignoreMethodList = Arrays.asList(
            "isFrozen", "getCallbacks", "getTargetSource", "getTargetClass",
            "getProxiedInterfaces", "getAdvisors", "isProxyTargetClass",
            "isExposeProxy", "isPreFiltered", "getDecoratedClass", "getClass",
            "getContextClassLoader", "getAllStackTraces", "getDefaultUncaughtExceptionHandler",
            "getUncaughtExceptionHandler", "getClassLoader", "getClientList", "getThreadGroup",
            "getApplicationListeners", "getBeanFactoryPostProcessors", "getProtocolResolvers",
            "getParentLogger"
    );

    /**
     * query AppCtx
     *
     * @param request HttpServletRequest
     * @param nameOpt, optional bean name
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/query-app-ctx",
            "/v1/query-app-ctx/{nameOpt}",
    }, method = RequestMethod.GET)
    public ResponseEntity<?> queryAppCtx(
            HttpServletRequest request,
            @PathVariable Optional<String> nameOpt) {

        Context context = new Context(false);
        Request.process(context, request);

        Map<String, Object> data = new LinkedHashMap<String, Object>();

        for (Method method : AppCtx.class.getMethods()) {

            String appCtxFName = method.getName();

            if (Modifier.isStatic(method.getModifiers()) && appCtxFName.startsWith("get")) {

                String beanName = appCtxFName.substring(3, appCtxFName.length());

                if (nameOpt.isPresent()) {
                    if (!beanName.equalsIgnoreCase(nameOpt.get())) {
                        continue;
                    }
                }

                try {
                    Map<String, Object> map = new LinkedHashMap<String, Object>();

                    Object object = method.invoke(null);

                    if (object == null) continue;

                    for (Method objMethod : object.getClass().getMethods()) {

                        if (objMethod.getParameterCount() > 0) continue;
                        if (objMethod.getReturnType().equals("void")) continue;

                        String objFName = objMethod.getName();
                        if (ignoreMethodList.contains(objFName)) continue;

                        String type = objMethod.getReturnType().getName();

                        String name = null;
                        if (objFName.startsWith("get")) {
                            name = objFName.substring(3, objFName.length());
                        } else if (objFName.startsWith("is")) {
                            name = objFName.substring(2, objFName.length());
                        }
                        if (name == null) continue;

                        if (type.indexOf('.') == -1 || type.startsWith("java.lang.") || type.startsWith("java.util.")) {
                            try {
                                Object value = objMethod.invoke(object);
                                map.put(name, value);
                            } catch (Exception e) {
                                System.out.println("Call " + beanName + "." + objFName);
                                e.printStackTrace();
                            }
                        }
                    }

                    data.put(beanName, map);

                } catch (Exception e) {
                    System.out.println("Call AppCtxx." + appCtxFName);
                    e.printStackTrace();
                }
            }
        }

        return Response.send(context, data);
    }
}
