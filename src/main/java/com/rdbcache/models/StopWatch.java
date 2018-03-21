/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.models;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name="rdbcache_stopwatch")
public class StopWatch implements Serializable {

    private static final long serialVersionUID = 20180316L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long monitorId;

    private String type;

    private String action;

    private Long threadId;

    private Long duration;

    private Long startedAt;

    private Long endedAt;

    public StopWatch(String type, String action) {
        threadId = Thread.currentThread().getId();
        this.startedAt = System.nanoTime();
        this.type = type;
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

    @Override
    public String toString() {
        return "StopWatch{" +
                "id=" + id +
                ", monitorId=" + monitorId +
                ", threadId=" + threadId +
                ", type='" + type + '\'' +
                ", action='" + action + '\'' +
                ", duration=" + duration +
                ", startedAt=" + startedAt +
                ", endedAt=" + endedAt +
                '}';
    }
}
