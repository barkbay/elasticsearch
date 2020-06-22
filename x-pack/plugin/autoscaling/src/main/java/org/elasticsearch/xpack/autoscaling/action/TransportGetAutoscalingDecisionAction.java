/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecisions;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicyMetadata;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class TransportGetAutoscalingDecisionAction extends TransportMasterNodeAction<
    GetAutoscalingDecisionAction.Request,
    GetAutoscalingDecisionAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetAutoscalingDecisionAction.class);

    private final ClusterInfoService clusterInfoService;
    private final AllocationDeciders allocationDeciders;
    private final ShardsAllocator shardAllocator;


    @Inject
    public TransportGetAutoscalingDecisionAction(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterInfoService clusterInfoService,
        AllocationDeciders allocationDeciders,
        ShardsAllocator shardAllocator
    ) {
        super(
            GetAutoscalingDecisionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetAutoscalingDecisionAction.Request::new,
            indexNameExpressionResolver
        );
        this.clusterInfoService = clusterInfoService;
        this.allocationDeciders = allocationDeciders;
        this.shardAllocator = shardAllocator;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetAutoscalingDecisionAction.Response read(final StreamInput in) throws IOException {
        return new GetAutoscalingDecisionAction.Response(in);
    }

    private AutoscalingDeciderContext newAutoscalingDeciderContext(final ClusterState state)  {
        return new AutoscalingDeciderContext() {
            @Override
            public ClusterState state() {
                return state;
            }

            @Override
            public ClusterInfo info() {
                return clusterInfoService.getClusterInfo();
            }

            @Override
            public ShardsAllocator shardsAllocator() {
                return shardAllocator;
            }

            @Override
            public AllocationDeciders allocationDeciders() {
                return allocationDeciders;
            }
        };
    }

    @Override
    protected void masterOperation(
        final Task task,
        final GetAutoscalingDecisionAction.Request request,
        final ClusterState state,
        final ActionListener<GetAutoscalingDecisionAction.Response> listener
    ) {
        final AutoscalingMetadata metadata;
        if (state.metadata().custom(AutoscalingMetadata.NAME) != null) {
            metadata = state.metadata().custom(AutoscalingMetadata.NAME);
        } else {
            // we will reject the request below when we try to look up the policy by name
            metadata = AutoscalingMetadata.EMPTY;
        }
        final SortedMap<String, AutoscalingPolicyMetadata> policies =  metadata.policies();
        final Set<AutoscalingPolicy> p = policies.keySet().stream().map(s -> policies.get(s).policy()).collect(Collectors.toSet());
        final SortedMap<String, AutoscalingDecisions> result = new TreeMap<>();
        p.stream().map(AutoscalingPolicy::deciders).forEach(stringAutoscalingDeciderSortedMap
            -> stringAutoscalingDeciderSortedMap.forEach((s, autoscalingDecider)
            -> result.put(s, new AutoscalingDecisions(List.of(autoscalingDecider.scale(newAutoscalingDeciderContext(state))) ))));

        result.forEach( (k,v) -> logger.info("TransportGetAutoscalingDecisionAction(" + k + "," + v +")")) ;


        listener.onResponse(new GetAutoscalingDecisionAction.Response(Collections.unmodifiableSortedMap(result)));
    }

    @Override
    protected ClusterBlockException checkBlock(final GetAutoscalingDecisionAction.Request request, final ClusterState state) {
        return null;
    }

}
