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
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name="rdbcache_monitor")
public class Monitor implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private String name;

    private Long threadId;

    private Long duration;

    private Long mainDuration;

    private Long startedAt;

    private Long endedAt;

    private String traceId;

    private String builtInfo;

    public Monitor(String name, String type, String action) {
        this.threadId = Thread.currentThread().getId();
        this.startedAt = System.nanoTime();
        this.name = name;
        startFirstStopWatch(type, action);
    }

    public Monitor() {
        this.threadId = Thread.currentThread().getId();
        this.startedAt = System.nanoTime();
        startFirstStopWatch();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public void setDuration(Long mainDuration) {
        this.duration = mainDuration;
    }

    public Long getMainDuration() {
        return mainDuration;
    }

    public void setMainDuration(Long mainDuration) {
        this.mainDuration = mainDuration;
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

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getBuiltInfo() {
        return builtInfo;
    }

    public void setBuiltInfo(String builtInfo) {
        this.builtInfo = builtInfo;
    }

    public void setTypeAndAction(String type, String action) {
        if (watches == null || watches.size() == 0) {
            return;
        }
        StopWatch watch = watches.get(0);
        watch.setType(type);
        watch.setAction(action);
    }

    public void startNow() {
        this.startedAt = System.nanoTime();
    }

    public Long stopNow() {
        if (endedAt != null) {
            return duration;
        }
        if (watches != null && watches.size() > 0) {
            for (StopWatch watch : watches) {
                watch.stopNow();
            }
            StopWatch watch = watches.get(0);
            mainDuration = watch.stopNow();
        }
        endedAt = System.nanoTime();
        duration = endedAt - startedAt;
        return duration;
    }

    public void startFirstStopWatch(String type, String action) {
        if (watches == null) {
            watches = new ArrayList<>();
        } else {
            watches.clear();
        }
        watches.add(new StopWatch(type, action));
    }

    public void startFirstStopWatch() {
        if (watches == null) {
            watches = new ArrayList<>();
        } else {
            watches.clear();
        }
        watches.add(new StopWatch());
    }

    public StopWatch getFirstStopWatch() {
        if (watches == null || watches.size() == 0) {
            return null;
        }
        return watches.get(0);
    }

    public Long stopFirstStopWatch() {

        if (watches == null || watches.size() == 0) {
            return null;
        }
        StopWatch watch = watches.get(0);
        mainDuration = watch.stopNow();
        return mainDuration;
    }

    //@OneToMany
    @Transient
    private List<StopWatch> watches;

    public List<StopWatch> getStopWatches() {
        return watches;
    }

    public void setStopWatches(List<StopWatch> watches) {
        this.watches = watches;
    }

    public StopWatch startStopWatch(String type, String action) {
        StopWatch stopWatch = new StopWatch(type, action);
        getStopWatches().add(stopWatch);
        return stopWatch;
    }

    @Override
    public String toString() {
        return "Monitor{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", traceId='" + traceId + '\'' +
                ", threadId=" + threadId +
                ", duration=" + duration +
                ", mainDuration=" + mainDuration +
                ", startedAt=" + startedAt +
                ", endedAt=" + endedAt +
                '}';
    }
}
