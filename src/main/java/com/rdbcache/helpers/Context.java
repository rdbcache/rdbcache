/*
 * Copyright (c) 2017-2018, Sam Wen <sam underscore wen at yahoo dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of rdbcache nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.rdbcache.helpers;

import com.rdbcache.Application;
import com.rdbcache.models.KvPair;
import com.rdbcache.models.Monitor;
import com.rdbcache.models.StopWatch;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;

public class Context {

    private List<KvPair> pairs = new ArrayList<KvPair>();

    private Monitor monitor;

    private Boolean sendValue = true;
    
    private String traceId;

    private Boolean monitorEnabled = false;

    public Context(String id, String value, Boolean sendValue) {
        this.sendValue = sendValue;
        KvPair pair = null;
        if (id.equals("*")) {
            pair = new KvPair(Utils.generateId(), "data", value);
        } else {
            pair = new KvPair(id, "data", value);
        }
        pairs.add(pair);
        traceId = Utils.generateId();
        monitor = new Monitor();
    }

    public Context(String id, Boolean sendValue) {
        this.sendValue = sendValue;
        KvPair pair = null;
        if (id.equals("*")) {
            pair = new KvPair(Utils.generateId());
        } else {
            pair = new KvPair(id);
        }
        pairs.add(pair);
        traceId = Utils.generateId();
        monitor = new Monitor();
    }

    public Context(String id, String traceId) {
        this.sendValue = false;
        KvPair pair = new KvPair(id);
        pairs.add(pair);
        this.traceId = traceId;
        monitor = new Monitor();
    }

    // for monitoring purpose
    public Context(Monitor monitor, List<KvPair> pairs) {
        this.monitor = monitor;
        this.pairs = pairs;
        traceId = monitor.getTraceId();
    }

    public Context(Boolean sendValue) {
        this.sendValue = sendValue;
        traceId = Utils.generateId();
        monitor = new Monitor();
    }

    public void enableMonitor(HttpServletRequest request) {
        monitorEnabled = true;
        monitor.setName(request.getRequestURI());
        monitor.setTypeAndAction("http", "process");
        monitor.setTraceId(traceId);
    }

    public void enableMonitor(String name, String type, String action) {
        monitorEnabled = true;
        monitor.setName(name);
        monitor.setTypeAndAction(type, action);
        monitor.setTraceId(traceId);
    }

    public List<KvPair> getPairs() {
        return pairs;
    }

    public void setPairs(List<KvPair> pairs) {
        this.pairs = pairs;
    }

    public void setPair(KvPair pair) {
        pairs.clear();
        pairs.add(pair);
    }

    public KvPair getPair() {
        if (pairs.size() == 0) {
            return null;
        }
        return pairs.get(0);
    }

    public Monitor getMonitor() {
        return monitor;
    }

    public void setMonitor(Monitor monitor) {
        this.monitor = monitor;
    }

    public Boolean ifSendValue() {
        return sendValue;
    }

    public void setSendValue(Boolean sendValue) {
        this.sendValue = sendValue;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    // for monitoring purpose
    public Context getCloneWith(String id) {
        KvPair pair = new KvPair(id);
        List<KvPair> ppairs = new ArrayList<KvPair>();
        ppairs.add(pair);
        return new Context(monitor, ppairs);
    }

    // for monitoring purpose
    public Context getCloneWith(String id, String vaule) {
        KvPair pair = new KvPair(id, "data", vaule);
        List<KvPair> ppairs = new ArrayList<KvPair>();
        ppairs.add(pair);
        return new Context(monitor, ppairs);
    }

    // for monitoring purpose
    public Context getCloneWith(KvPair pair) {
        List<KvPair> ppairs = new ArrayList<KvPair>();
        ppairs.add(pair);
        return new Context(monitor, ppairs);
    }

    // for monitoring purpose
    public Context getCloneWith(List<KvPair> pairs) {
        return new Context(monitor, pairs);
    }

    public StopWatch startStopWatch(String type, String action) {
        if (!monitorEnabled) {
            return null;
        }
        return monitor.startStopWatch(type, action);
    }

    public boolean saveMonitorData() {

        if (!monitorEnabled || monitor == null) {
            return false;
        }
        monitorEnabled = false;

        monitor.stopNow();
        monitor.setBuiltInfo(Application.versionInfo.getBriefInfo());

        Monitor result = AppCtx.getMonitorRepo().save(monitor);
        if (result == null) {
            return false;
        }

        List<StopWatch> watches = monitor.getStopWatches();
        if (watches != null && watches.size() > 0) {
            for (StopWatch watch: watches) {
                watch.setMonitorId(result.getId());
                AppCtx.getStopWatchRepo().save(watch);
            }
        }
        return true;
    }

    synchronized public void closeMonitor() {
        if (monitor == null) {
            return;
        }
        monitor.stopNow();
        saveMonitorData();
        monitor = null;
    }

    public Long stopFirstStopWatch() {
        if (monitor == null) {
            return null;
        }
        return monitor.stopFirstStopWatch();
    }

    public void logTraceMessage(String message) {
        if (traceId != null) {
            StackTraceElement[] trace = Thread.currentThread().getStackTrace();
            AppCtx.getDbaseOps().logTraceMessage(traceId, message, trace);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        closeMonitor();
        super.finalize();
    }
}
