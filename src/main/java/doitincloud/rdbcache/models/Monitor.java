/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.models;

import com.fasterxml.jackson.annotation.JsonInclude;

import javax.persistence.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Monitor {

    private Long id;

    private String name = "";

    @Column(name="thread_id")
    private Long threadId;

    private Long duration;

    @Column(name="main_duration")
    private Long mainDuration;

    @Column(name="started_at")
    private Long startedAt;

    @Column(name="ended_at")
    private Long endedAt;

    @Column(name="trace_id")
    private String traceId;

    @Column(name="built_info")
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

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public Long getMainDuration() {
        if (mainDuration == null) {
            stopFirstStopWatch();
        }
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
            for (StopWatch watch: watches) {
                watch.stopNow();
            }
        }
        endedAt = System.nanoTime();
        duration = endedAt - startedAt;
        return duration;
    }

    private void startFirstStopWatch(String type, String action) {
        if (watches == null) {
            watches = new ArrayList<>();
        } else {
            watches.clear();
        }
        watches.add(new StopWatch(type, action));
    }

    private void startFirstStopWatch() {
        if (watches == null) {
            watches = new ArrayList<>();
        } else {
            watches.clear();
        }
        watches.add(new StopWatch());
    }

    private StopWatch getFirstStopWatch() {
        if (watches == null || watches.size() == 0) {
            return null;
        }
        return watches.get(0);
    }

    private void stopFirstStopWatch() {
        if (watches == null || watches.size() == 0) {
            return;
        }
        StopWatch watch = watches.get(0);
        mainDuration = watch.stopNow();
    }

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
}
