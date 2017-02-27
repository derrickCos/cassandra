/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.concurrent;

import java.util.EnumMap;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.DatabaseDescriptor.*;


/**
 * This class manages executor services for Messages recieved: each Message requests
 * running on a specific "stage" for concurrency control; hence the Map approach,
 * even though stages (executors) are not created dynamically.
 */
public class StageManager
{
    private static final Logger logger = LoggerFactory.getLogger(StageManager.class);

    private static final EnumMap<Stage, LocalAwareExecutorService> stages = new EnumMap<Stage, LocalAwareExecutorService>(Stage.class);

    // The RX schedulers derived from the stages executors
    private static final EnumMap<Stage, Scheduler> schedulers = new EnumMap<>(Stage.class);

    public static final long KEEPALIVE = 60; // seconds to keep "extra" threads alive for when idle

    static
    {
        stages.put(Stage.MUTATION, multiThreadedStage(Stage.MUTATION, getConcurrentWriters()));
        stages.put(Stage.COUNTER_MUTATION, multiThreadedStage(Stage.COUNTER_MUTATION, getConcurrentCounterWriters()));
        stages.put(Stage.VIEW_MUTATION, multiThreadedStage(Stage.VIEW_MUTATION, getConcurrentViewWriters()));
        stages.put(Stage.READ, multiThreadedStage(Stage.READ, getConcurrentReaders()));
        stages.put(Stage.REQUEST_RESPONSE, multiThreadedStage(Stage.REQUEST_RESPONSE, FBUtilities.getAvailableProcessors()));
        stages.put(Stage.INTERNAL_RESPONSE, multiThreadedStage(Stage.INTERNAL_RESPONSE, FBUtilities.getAvailableProcessors()));
        // the rest are all single-threaded
        stages.put(Stage.GOSSIP, new JMXEnabledThreadPoolExecutor(Stage.GOSSIP));
        stages.put(Stage.ANTI_ENTROPY, new JMXEnabledThreadPoolExecutor(Stage.ANTI_ENTROPY));
        stages.put(Stage.MIGRATION, new JMXEnabledThreadPoolExecutor(Stage.MIGRATION));
        stages.put(Stage.MISC, new JMXEnabledThreadPoolExecutor(Stage.MISC));
        stages.put(Stage.READ_REPAIR, multiThreadedStage(Stage.READ_REPAIR, FBUtilities.getAvailableProcessors()));
        stages.put(Stage.TRACING, tracingExecutor());
        stages.put(Stage.CONTINUOUS_PAGING, multiThreadedStage(Stage.CONTINUOUS_PAGING, getContinuousPagingThreads()));

        // add the corresponding scheduler to each stage
        stages.forEach((stage, executor) -> schedulers.put(stage, Schedulers.from(executor)));
    }

    private static LocalAwareExecutorService tracingExecutor()
    {
        RejectedExecutionHandler reh = new RejectedExecutionHandler()
        {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
            {
                MessagingService.instance().incrementDroppedMessages(MessagingService.Verb._TRACE);
            }
        };
        return new TracingExecutor(1,
                                   1,
                                   KEEPALIVE,
                                   TimeUnit.SECONDS,
                                   new ArrayBlockingQueue<Runnable>(1000),
                                   new NamedThreadFactory(Stage.TRACING.getJmxName()),
                                   reh);
    }

    private static JMXEnabledThreadPoolExecutor multiThreadedStage(Stage stage, int numThreads)
    {
        return new JMXEnabledThreadPoolExecutor(numThreads,
                                                KEEPALIVE,
                                                TimeUnit.SECONDS,
                                                new LinkedBlockingQueue<Runnable>(),
                                                new NamedThreadFactory(stage.getJmxName()),
                                                stage.getJmxType());
    }

    /**
     * Retrieve a stage from the StageManager
     * @param stage name of the stage to be retrieved.
     */
    public static LocalAwareExecutorService getStage(Stage stage)
    {
        return stages.get(stage);
    }

    /**
     * Retrieve the RX scheduler corresponding to a stage.
     *
     * @param stage name of the stage to be retrieved.
     * @return RX scheduler executing on the stage
     */
    public static Scheduler getScheduler(Stage stage)
    {
        return schedulers.get(stage);
    }

    /**
     * This method shuts down all registered stages.
     */
    public static void shutdownNow()
    {
        for (Stage stage : Stage.values())
        {
            StageManager.stages.get(stage).shutdownNow();
        }
    }

    /**
     * The executor used for tracing.
     */
    private static class TracingExecutor extends ThreadPoolExecutor implements LocalAwareExecutorService
    {
        public TracingExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler)
        {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        }

        public void execute(Runnable command, ExecutorLocals locals)
        {
            assert locals == null;
            super.execute(command);
        }

        public void maybeExecuteImmediately(Runnable command)
        {
            execute(command);
        }
    }
}
