/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.helpers;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.models.Monitor;
import com.rdbcache.models.StopWatch;
import com.rdbcache.repositories.DbaseRepo;
import com.rdbcache.repositories.MonitorRepo;
import com.rdbcache.repositories.StopWatchRepo;
import com.rdbcache.services.DbaseOps;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.logging.Logger;

public class Context {

    private String action;

    private Monitor monitor;

    private Boolean sendValue = true;

    private Boolean batch = false;

    private String traceId;

    private Boolean monitorEnabled = false;

    public Context(Boolean sendValue) {
        this.sendValue = sendValue;
        traceId = Utils.generateId();
        monitor = new Monitor();
        action = Thread.currentThread().getStackTrace()[2].getMethodName();
    }

    public Context(Boolean sendValue, Boolean batch) {
        this.sendValue = sendValue;
        this.batch = batch;
        traceId = Utils.generateId();
        monitor = new Monitor();
        action = Thread.currentThread().getStackTrace()[2].getMethodName();
    }

    public Context(String traceId) {
        this.sendValue = false;
        this.traceId = traceId;
        monitor = new Monitor();
        action = Thread.currentThread().getStackTrace()[2].getMethodName();
    }

    public Context() {
        this.sendValue = false;
        traceId = Utils.generateId();
        monitor = new Monitor();
        action = Thread.currentThread().getStackTrace()[2].getMethodName();
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

    public boolean isMonitorEnabled() {
        return monitorEnabled;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Monitor getMonitor() {
        return monitor;
    }

    public void setMonitor(Monitor monitor) {
        this.monitor = monitor;
    }

    public Boolean isSendValue() {
        return sendValue;
    }

    public void setSendValue(Boolean sendValue) {
        this.sendValue = sendValue;
    }

    public Boolean isBatch() {
        return batch;
    }

    public void setBatch(Boolean batch) {
        this.batch = batch;
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

    synchronized public void closeMonitor() {
        if (monitor == null) {
            return;
        }
        monitor.stopNow();
        DbaseOps dbaseOps = AppCtx.getDbaseOps();
        if (dbaseOps != null) {
            try {
                dbaseOps.saveMonitor(this);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
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
