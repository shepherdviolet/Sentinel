/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.dashboard.repository.metric;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;

import com.alibaba.csp.sentinel.dashboard.datasource.entity.MetricEntity;
import com.alibaba.csp.sentinel.util.StringUtil;

import org.springframework.stereotype.Component;

/**
 * Caches metrics data in a period of time in memory.
 *
 * @author Carpenter Lee
 * @author Eric Zhao
 */
@Component
public class InMemoryMetricsRepository implements MetricsRepository<MetricEntity> {

    //Data is saved for 30 minutes
    private static final long MAX_METRIC_LIVE_TIME_MS = 1000 * 60 * 30;

    //There is a problem with this implementation. The time unit of the statistical sample must be seconds.
    private static final long SECOND_MILLIS = 1000;
    private static final int MAX_METRIC_SAMPLES = (int) (MAX_METRIC_LIVE_TIME_MS / SECOND_MILLIS);

    /**
     * {@code app -> resource -> timestamp -> metric}
     */
    private Map<String, Map<String, AtomicReferenceArray<MetricEntity>>> allMetrics = new ConcurrentHashMap<>();



    @Override
    public synchronized void save(MetricEntity entity) {
        if (entity == null || StringUtil.isBlank(entity.getApp())) {
            return;
        }
        allMetrics.computeIfAbsent(entity.getApp(), e -> new ConcurrentHashMap<>(16))
                .computeIfAbsent(entity.getResource(), e -> new AtomicReferenceArray<>(MAX_METRIC_SAMPLES))
                .set(getIndex(entity), entity);
    }

    /**
     * calculate index
     */
    private int getIndex(MetricEntity entity) {
        return (int)(entity.getTimestamp().getTime() / SECOND_MILLIS) % MAX_METRIC_SAMPLES;
    }

    @Override
    public synchronized void saveAll(Iterable<MetricEntity> metrics) {
        if (metrics == null) {
            return;
        }
        metrics.forEach(this::save);
    }

    @Override
    public synchronized List<MetricEntity> queryByAppAndResourceBetween(String app, String resource,
                                                                        long startTime, long endTime) {
        List<MetricEntity> results = new ArrayList<>();
        if (StringUtil.isBlank(app)) {
            return results;
        }
        Map<String, AtomicReferenceArray<MetricEntity>> resourceMap = allMetrics.get(app);
        if (resourceMap == null) {
            return results;
        }
        AtomicReferenceArray<MetricEntity> metricsArray = resourceMap.get(resource);
        if (metricsArray == null) {
            return results;
        }
        //This code can be optimized, Reduce the search range based on startTime and endTime
        for (int i = 0 ; i < MAX_METRIC_SAMPLES ; i++) {
            MetricEntity entity = metricsArray.get(i);
            if (entity == null) {
                continue;
            }
            long time = entity.getTimestamp().getTime();
            if (time >= startTime && time <= endTime) {
                results.add(entity);
            }
        }
        return results;
    }

    @Override
    public List<String> listResourcesOfApp(String app) {
        List<String> results = new ArrayList<>();
        if (StringUtil.isBlank(app)) {
            return results;
        }
        // resource -> timestamp -> metric
        Map<String, AtomicReferenceArray<MetricEntity>> resourceMap = allMetrics.get(app);
        if (resourceMap == null) {
            return results;
        }
        //As long as there is data in 5 minutes, it will be displayed on the dashboard.
        final long minTimeMs = System.currentTimeMillis() - 1000 * 60 * 5;
        Map<String, MetricEntity> resourceCount = new ConcurrentHashMap<>(32);

        for (Entry<String, AtomicReferenceArray<MetricEntity>> resourceMetrics : resourceMap.entrySet()) {
            AtomicReferenceArray<MetricEntity> metricsArray = resourceMetrics.getValue();
            //This code can be optimized, Reduce the search range based on minTimeMs
            for (int i = 0 ; i < MAX_METRIC_SAMPLES ; i++) {
                MetricEntity entity = metricsArray.get(i);
                if (entity == null) {
                    continue;
                }
                long time = entity.getTimestamp().getTime();
                if (time < minTimeMs) {
                    continue;
                }
                if (resourceCount.containsKey(resourceMetrics.getKey())) {
                    MetricEntity oldEntity = resourceCount.get(resourceMetrics.getKey());
                    oldEntity.addPassQps(entity.getPassQps());
                    oldEntity.addRtAndSuccessQps(entity.getRt(), entity.getSuccessQps());
                    oldEntity.addBlockQps(entity.getBlockQps());
                    oldEntity.addExceptionQps(entity.getExceptionQps());
                    oldEntity.addCount(1);
                } else {
                    resourceCount.put(resourceMetrics.getKey(), MetricEntity.copyOf(entity));
                }
            }
        }
        // Order by last minute b_qps DESC.
        return resourceCount.entrySet()
            .stream()
            .sorted((o1, o2) -> {
                MetricEntity e1 = o1.getValue();
                MetricEntity e2 = o2.getValue();
                int t = e2.getBlockQps().compareTo(e1.getBlockQps());
                if (t != 0) {
                    return t;
                }
                return e2.getPassQps().compareTo(e1.getPassQps());
            })
            .map(Entry::getKey)
            .collect(Collectors.toList());
    }
}
