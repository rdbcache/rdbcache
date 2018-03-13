/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.helpers;

import com.rdbcache.Application;
import com.rdbcache.configs.AppCtx;
import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.models.KvPair;
import com.rdbcache.models.Monitor;
import com.rdbcache.models.StopWatch;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;

public class Context {

    private Monitor monitor;

    private Boolean sendValue = true;
    
    private String traceId;

    private Boolean monitorEnabled = false;

    public Context(Boolean sendValue) {
        this.sendValue = sendValue;
        traceId = Utils.generateId();
        monitor = new Monitor();
    }

    public Context(String traceId) {
        this.sendValue = false;
        this.traceId = traceId;
        monitor = new Monitor();
    }

    // for monitoring purpose
    public Context(Monitor monitor) {
        this.monitor = monitor;
        traceId = monitor.getTraceId();
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
        if (traceId == null) {
            return;
        }
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        AppCtx.getDbaseOps().logTraceMessage(traceId, message, trace);
    }

    @Override
    protected void finalize() throws Throwable {
        closeMonitor();
        super.finalize();
    }
}
