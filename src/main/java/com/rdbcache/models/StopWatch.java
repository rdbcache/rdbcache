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

package com.rdbcache.models;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name="rdbcache_stopwatch")
public class StopWatch implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    //@ManyToOne
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
