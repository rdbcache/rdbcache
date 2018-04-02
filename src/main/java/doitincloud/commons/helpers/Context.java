/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.commons.helpers;

import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.rdbcache.configs.PropCfg;
import doitincloud.rdbcache.models.Monitor;
import doitincloud.rdbcache.models.StopWatch;
import doitincloud.rdbcache.services.DbaseOps;

import javax.servlet.http.HttpServletRequest;

public class Context {

    private String action;

    private Monitor monitor;

    private Boolean sendValue = true;

    private Boolean batch = false;

    private String traceId;

    private Boolean monitorEnabled = PropCfg.getEnableMonitor();

    private Boolean sync = PropCfg.getDefaultSync();

    private Long duration;

    public Context(Boolean sendValue) {
        this.sendValue = sendValue;
        traceId = Utils.generateId();
        StackTraceElement element = Thread.currentThread().getStackTrace()[2];
        action = element.getMethodName();
        if (sync) action += "/sync";
        monitor = new Monitor(element.getFileName(), element.getClassName(), action);
    }

    public Context(Boolean sendValue, Boolean batch) {
        this.sendValue = sendValue;
        this.batch = batch;
        traceId = Utils.generateId();
        StackTraceElement element = Thread.currentThread().getStackTrace()[2];
        action = element.getMethodName();
        if (sync) action += "/sync";
        monitor = new Monitor(element.getFileName(), element.getClassName(), action);
    }

    public Context(String traceId) {
        this.sendValue = false;
        this.traceId = traceId;
        StackTraceElement element = Thread.currentThread().getStackTrace()[2];
        action = element.getMethodName();
        if (sync) action += "/sync";
        monitor = new Monitor(element.getFileName(), element.getClassName(), action);
    }

    public Context() {
        this.sendValue = false;
        traceId = Utils.generateId();
        action = Thread.currentThread().getStackTrace()[2].getMethodName();
        StackTraceElement element = Thread.currentThread().getStackTrace()[2];
        action = element.getMethodName();
        if (sync) action += "-sync";
        monitor = new Monitor(element.getFileName(), element.getClassName(), action);
    }

    public void enableMonitor(HttpServletRequest request) {
        monitorEnabled = true;
        if (monitor == null) {
            monitor = new Monitor(request.getRequestURI(), "http", action);
        } else {
            monitor.setName(request.getRequestURI());
            monitor.setTypeAndAction("http", action);
        }
        monitor.setTraceId(traceId);
    }

    public void enableMonitor(String name, String type, String action) {
        monitorEnabled = true;
        if (monitor == null) {
            monitor = new Monitor(name, type, action);
        } else {
            monitor.setName(name);
            monitor.setTypeAndAction(type, action);
        }
        monitor.setTraceId(traceId);
    }

    public boolean isMonitorEnabled() {
        if (monitor == null) {
            monitorEnabled = false;
        }
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

    public Boolean isSync() {
        return sync;
    }

    public void setSync(Boolean sync) {
        this.sync = sync;
    }

    public Long getDuration() {
        if (duration == null && monitor != null) {
            duration = monitor.getMainDuration();
        }
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public StopWatch startStopWatch(String type, String action) {
        if (!monitorEnabled || monitor == null) {
            return null;
        }
        return monitor.startStopWatch(type, action);
    }

    synchronized public void closeMonitor() {
        if (monitor == null) {
            return;
        }
        duration = monitor.getMainDuration();
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
