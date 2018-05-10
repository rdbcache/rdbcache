/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.models;

import com.fasterxml.jackson.annotation.JsonInclude;

import javax.persistence.*;
import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class StopWatch {

    private Long id;

    @Column(name="monitor_id")
    private Long monitorId;

    private String type = "";

    private String action;

    @Column(name="thread_id")
    private Long threadId;

    private Long duration;

    @Column(name="started_at")
    private Long startedAt;

    @Column(name="ended_at")
    private Long endedAt;

    public StopWatch(String type, String action) {
        threadId = Thread.currentThread().getId();
        this.startedAt = System.nanoTime();
        this.type = (type.length() <= 16 ? type : type.substring(type.length() - 16));
        this.action = action;
    }

    public StopWatch() {
        threadId = Thread.currentThread().getId();
        this.startedAt = System.nanoTime();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getMonitorId() {
        return monitorId;
    }

    public void setMonitorId(Long monitorId) {
        this.monitorId  = monitorId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Long getThreadId() {
        return threadId;
    }

    public void setThreadId(Long threadId) {
        this.threadId = threadId;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public Long getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Long startedAt) {
        this.startedAt = startedAt;
    }

    public Long getEndedAt() {
        return endedAt;
    }

    public void setEndedAt(Long endedAt) {
        this.endedAt = endedAt;
    }

    public void startNow() {
        this.startedAt = System.nanoTime();
    }

    public Long stopNow() {
        if (endedAt != null) {
            return duration;
        }
        endedAt = System.nanoTime();
        duration = endedAt - startedAt;
        return duration;
    }
}
