/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction.unified;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MonotonicClock;

/**
 * The adaptive compaction controller dynamically calculates the optimal scaling parameter W.
 * <p/>
 * Generally it tries to find a local minimum for the total IO cost that is projected
 * by the strategy. The projected IO cost is composed by two parts: the read amplification,
 * which is weighted by the number of partitions read by the user, and the write amplification, which
 * is weighted by the number of bytes inserted into memtables. Other parameters are also considered, such
 * as the cache miss rate and the time it takes to read and write from disk. See also the comments in
 * {@link CostsCalculator}.
 *
 * Design doc: TODO: link to design doc or SEP
 */
public class AdaptiveController extends Controller
{
    private static final Logger logger = LoggerFactory.getLogger(AdaptiveController.class);

    /** The starting value for the scaling parameter */
    static final String STARTING_SCALING_PARAMETER = "adaptive_starting_scaling_parameter";
    private static final int DEFAULT_STARTING_SCALING_PARAMETER = Integer.getInteger(PREFIX + STARTING_SCALING_PARAMETER, 0);

    /** The minimum valid value for the scaling parameter */
    static final String MIN_SCALING_PARAMETER = "adaptive_min_scaling_parameter";
    static private final int DEFAULT_MIN_SCALING_PARAMETER = Integer.getInteger(PREFIX + MIN_SCALING_PARAMETER, -10);

    /** The maximum valid value for the scaling parameter */
    static final String MAX_SCALING_PARAMETER = "adaptive_max_scaling_parameter";
    static private final int DEFAULT_MAX_SCALING_PARAMETER = Integer.getInteger(PREFIX + MAX_SCALING_PARAMETER, 36);

    /** The interval for periodically checking the optimal value for the scaling parameter */
    static final String INTERVAL_SEC = "adaptive_interval_sec";
    static private final int DEFAULT_INTERVAL_SEC = Integer.getInteger(PREFIX + INTERVAL_SEC, 300);

    /** The gain is a number between 0 and 1 used to determine if a new choice of the scaling parameter is better than the current one */
    static final String THRESHOLD = "adaptive_threshold";
    private static final double DEFAULT_THRESHOLD = Double.parseDouble(System.getProperty(PREFIX + THRESHOLD, "0.15"));

    /** Below the minimum cost we don't try to optimize the scaling parameter, we consider the current scaling parameter good enough. This is necessary because the cost
     * can vanish to zero when there are neither reads nor writes and right now we don't know how to handle this case.  */
    static final String MIN_COST = "adaptive_min_cost";
    static private final int DEFAULT_MIN_COST = Integer.getInteger(PREFIX + MIN_COST, 1000);

    /** The maximum number of concurrent Adaptive Compactions */
    static final String MAX_ADAPTIVE_COMPACTIONS = "max_adaptive_compactions";
    private static final int DEFAULT_MAX_ADAPTIVE_COMPACTIONS = Integer.getInteger(PREFIX + MAX_ADAPTIVE_COMPACTIONS, 5);

    private final int intervalSec;
    private final int minScalingParameter;
    private final int maxScalingParameter;
    private final double threshold;
    private final int minCost;
    /** Protected by the synchronized block in UnifiedCompactionStrategy#getNextBackgroundTasks */
    private int[] scalingParameters;
    private int[] previousScalingParameters;
    private volatile long lastChecked;
    private final int maxAdaptiveCompactions;

    @VisibleForTesting
    public AdaptiveController(MonotonicClock clock,
                              Environment env,
                              int[] scalingParameters,
                              int[] previousScalingParameters,
                              double[] survivalFactors,
                              long dataSetSizeMB,
                              int numShards,
                              long minSstableSizeMB,
                              long flushSizeOverrideMB,
                              double maxSpaceOverhead,
                              int maxSSTablesToCompact,
                              long expiredSSTableCheckFrequency,
                              boolean ignoreOverlapsInExpirationCheck,
                              boolean l0ShardsEnabled,
                              int intervalSec,
                              int minScalingParameter,
                              int maxScalingParameter,
                              double threshold,
                              int minCost,
                              int maxAdaptiveCompactions)
    {
        super(clock, env, survivalFactors, dataSetSizeMB, numShards, minSstableSizeMB, flushSizeOverrideMB, maxSpaceOverhead, maxSSTablesToCompact, expiredSSTableCheckFrequency, ignoreOverlapsInExpirationCheck, l0ShardsEnabled);

        this.scalingParameters = scalingParameters;
        this.previousScalingParameters = previousScalingParameters;
        this.intervalSec = intervalSec;
        this.minScalingParameter = minScalingParameter;
        this.maxScalingParameter = maxScalingParameter;
        this.threshold = threshold;
        this.minCost = minCost;
        this.maxAdaptiveCompactions = maxAdaptiveCompactions;
    }

    static Controller fromOptions(Environment env,
                                  double[] survivalFactors,
                                  long dataSetSizeMB,
                                  int numShards,
                                  long minSstableSizeMB,
                                  long flushSizeOverrideMB,
                                  double maxSpaceOverhead,
                                  int maxSSTablesToCompact,
                                  long expiredSSTableCheckFrequency,
                                  boolean ignoreOverlapsInExpirationCheck,
                                  boolean l0ShardsEnabled,
                                  Map<String, String> options)
    {
        int scalingParameter = options.containsKey(STARTING_SCALING_PARAMETER) ? Integer.parseInt(options.get(STARTING_SCALING_PARAMETER)) : DEFAULT_STARTING_SCALING_PARAMETER;
        int[] scalingParameters = new int[32];
        int[] previousScalingParameters = new int[32];
        //set the scaling parameter for each level to the starting scaling parameter or default scaling parameter
        Arrays.fill(scalingParameters, scalingParameter);
        Arrays.fill(previousScalingParameters, scalingParameter);
        int minScalingParameter = options.containsKey(MIN_SCALING_PARAMETER) ? Integer.parseInt(options.get(MIN_SCALING_PARAMETER)) : DEFAULT_MIN_SCALING_PARAMETER;
        int maxScalingParameter = options.containsKey(MAX_SCALING_PARAMETER) ? Integer.parseInt(options.get(MAX_SCALING_PARAMETER)) : DEFAULT_MAX_SCALING_PARAMETER;
        int intervalSec = options.containsKey(INTERVAL_SEC) ? Integer.parseInt(options.get(INTERVAL_SEC)) : DEFAULT_INTERVAL_SEC;
        double threshold = options.containsKey(THRESHOLD) ? Double.parseDouble(options.get(THRESHOLD)) : DEFAULT_THRESHOLD;
        int minCost = options.containsKey(MIN_COST) ? Integer.parseInt(options.get(MIN_COST)) : DEFAULT_MIN_COST;
        int maxAdaptiveCompactions = options.containsKey(MAX_ADAPTIVE_COMPACTIONS) ? Integer.parseInt(options.get(MAX_ADAPTIVE_COMPACTIONS)) : DEFAULT_MAX_ADAPTIVE_COMPACTIONS;

        return new AdaptiveController(MonotonicClock.preciseTime, env, scalingParameters, previousScalingParameters, survivalFactors, dataSetSizeMB, numShards, minSstableSizeMB, flushSizeOverrideMB, maxSpaceOverhead, maxSSTablesToCompact, expiredSSTableCheckFrequency, ignoreOverlapsInExpirationCheck, l0ShardsEnabled, intervalSec, minScalingParameter, maxScalingParameter, threshold, minCost, maxAdaptiveCompactions);
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        int scalingParameter = DEFAULT_STARTING_SCALING_PARAMETER;
        int minScalingParameter = DEFAULT_MIN_SCALING_PARAMETER;
        int maxScalingParameter = DEFAULT_MAX_SCALING_PARAMETER;

        String s;
        s = options.remove(STARTING_SCALING_PARAMETER);
        if (s != null)
            scalingParameter = Integer.parseInt(s);
        s = options.remove(MIN_SCALING_PARAMETER);
        if (s != null)
            minScalingParameter = Integer.parseInt(s);
        s = options.remove(MAX_SCALING_PARAMETER);
        if (s != null)
            maxScalingParameter = Integer.parseInt(s);

        if (minScalingParameter >= maxScalingParameter || scalingParameter < minScalingParameter || scalingParameter > maxScalingParameter)
            throw new ConfigurationException(String.format("Invalid configuration for the scaling parameter: %d, min: %d, max: %d", scalingParameter, minScalingParameter, maxScalingParameter));

        s = options.remove(INTERVAL_SEC);
        if (s != null)
        {
            int intervalSec = Integer.parseInt(s);
            if (intervalSec <= 0)
                throw new ConfigurationException(String.format("Invalid configuration for interval, it should be positive: %d", intervalSec));
        }
        s = options.remove(THRESHOLD);
        if (s != null)
        {
            double threshold = Double.parseDouble(s);
            if (threshold <= 0 || threshold > 1)
            {
                throw new ConfigurationException(String.format("Invalid configuration for threshold, it should be within (0,1]: %f", threshold));
            }
        }
        s = options.remove(MIN_COST);
        if (s != null)
        {
            int minCost = Integer.parseInt(s);
            if (minCost <= 0)
                throw new ConfigurationException(String.format("Invalid configuration for minCost, it should be positive: %d", minCost));
        }
        s = options.remove(MAX_ADAPTIVE_COMPACTIONS);
        if (s != null)
        {
            int maxAdaptiveCompactions = Integer.parseInt(s);
            if (maxAdaptiveCompactions < -1)
                throw new ConfigurationException(String.format("Invalid configuration for maxAdaptiveCompactions, it should be >= -1 (-1 for no limit): %d", maxAdaptiveCompactions));
        }
        return options;
    }

    @Override
    void startup(UnifiedCompactionStrategy strategy, CostsCalculator calculator)
    {
        super.startup(strategy, calculator);
        this.lastChecked = clock.now();
    }

    @Override
    public int getScalingParameter(int index)
    {
        if (index < 0)
            throw new IllegalArgumentException("Index should be >= 0: " + index);

        return index < scalingParameters.length ? scalingParameters[index] : scalingParameters[scalingParameters.length - 1];
    }

    @Override
    public int getPreviousScalingParameter(int index)
    {
        if (index < 0)
            throw new IllegalArgumentException("Index should be >= 0: " + index);

        return index < previousScalingParameters.length ? previousScalingParameters[index] : previousScalingParameters[previousScalingParameters.length - 1];
    }

    @Override
    @Nullable
    public CostsCalculator getCalculator()
    {
        return calculator;
    }

    public int getInterval()
    {
        return intervalSec;
    }

    public int getMinScalingParameter()
    {
        return minScalingParameter;
    }

    public int getMaxScalingParameter()
    {
        return maxScalingParameter;
    }

    public double getThreshold()
    {
        return threshold;
    }

    public int getMinCost()
    {
        return minCost;
    }

    @Override
    public int getMaxAdaptiveCompactions()
    {
        return maxAdaptiveCompactions;
    }

    /** Protected by the synchronized block in UnifiedCompactionStrategy#getNextBackgroundTasks */
    @Override
    public void onStrategyBackgroundTaskRequest()
    {
        if (!isRunning())
            return;

        long now = clock.now();
        if (now - lastChecked < TimeUnit.SECONDS.toNanos(intervalSec))
            return;

        try
        {
            maybeUpdate(now);
        }
        finally
        {
            lastChecked = now;
        }
    }

    /**
     * Maybe updates the scaling parameter according to the data size, read, and write costs.
     *
     * The scaling parameter calculation is based on current read and write query costs for the entire data size.
     * We use the entire data size instead of shard size here because query cost calculations do not take
     * sharding into account. Also, the same scaling parameter is going to be used across all shards.
     *
     * Protected by the synchronized block in UnifiedCompactionStrategy#getNextBackgroundTasks
     *
     * @param now current timestamp only used for debug logging
     */
    private void maybeUpdate(long now)
    {
        final long targetSize = Math.max(getDataSetSizeBytes(), (long) Math.ceil(calculator.spaceUsed()));

        final int RA = readAmplification(targetSize, scalingParameters[0]);
        final int WA = writeAmplification(targetSize, scalingParameters[0]);

        final double readCost = calculator.getReadCostForQueries(RA);
        final double writeCost = calculator.getWriteCostForQueries(WA);
        final double cost =  readCost + writeCost;

        if (cost <= minCost)
        {
            logger.debug("Adaptive compaction controller not updated, cost for current scaling parameter {} is below minimum cost {}: read cost: {}, write cost: {}\\nAverages: {}", scalingParameters[0], minCost, readCost, writeCost, calculator);
            return;
        }

        final double[] totCosts = new double[maxScalingParameter - minScalingParameter + 1];
        final double[] readCosts = new double[maxScalingParameter - minScalingParameter + 1];
        final double[] writeCosts = new double[maxScalingParameter - minScalingParameter + 1];
        int candScalingParameter = scalingParameters[0];
        double candCost = cost;

        for (int i = minScalingParameter; i <= maxScalingParameter; i++)
        {
            final int idx = i - minScalingParameter;
            if (i == scalingParameters[0])
            {
                readCosts[idx] = readCost;
                writeCosts[idx] = writeCost;
            }
            else
            {
                final int ra = readAmplification(targetSize, i);
                final int wa = writeAmplification(targetSize, i);

                readCosts[idx] = calculator.getReadCostForQueries(ra);
                writeCosts[idx] = calculator.getWriteCostForQueries(wa);
            }
            totCosts[idx] = readCosts[idx] + writeCosts[idx];
            // in case of a tie, for neg.ve scalingParameters we prefer higher scalingParameters (smaller WA), but not for pos.ve scalingParameters we prefer lower scalingParameters (more parallelism)
            if (totCosts[idx] < candCost || (i < 0 && totCosts[idx] == candCost))
            {
                candScalingParameter = i;
                candCost = totCosts[idx];
            }
        }

        logger.debug("Min cost: {}, min scaling parameter: {}, min sstable size: {}\nread costs: {}\nwrite costs: {}\ntot costs: {}\nAverages: {}",
                     candCost,
                     candScalingParameter,
                     FBUtilities.prettyPrintMemory(getMinSstableSizeBytes()),
                     Arrays.toString(readCosts),
                     Arrays.toString(writeCosts),
                     Arrays.toString(totCosts),
                     calculator);

        StringBuilder str = new StringBuilder(100);
        str.append("Adaptive compaction controller ");

        if (scalingParameters[0] != candScalingParameter && (cost - candCost) >= threshold * cost)
        {
            //scaling parameter is updated
            str.append("updated ").append(scalingParameters[0]).append(" -> ").append(candScalingParameter);
            this.previousScalingParameters[0] = scalingParameters[0]; //need to keep track of the previous scaling parameter for isAdaptive check
            this.scalingParameters[0] = candScalingParameter;
        }
        else if (scalingParameters[0] == candScalingParameter)
        {
            // only update the lowest level that is not equal to candScalingParameter
            // example: candScalingParameter = 4, scalingParameters = {4, 4, 12, 16} --> scalingParameters = {4, 4, 4, 16}
            // as a result, higher levels will be less prone to changes
            for (int i = 1; i < scalingParameters.length; i++)
            {
                if (scalingParameters[i] != candScalingParameter)
                {
                    str.append("updated for level ").append(i).append(": ").append(scalingParameters[i]).append(" -> ").append(candScalingParameter);
                    this.previousScalingParameters[i] = scalingParameters[i];
                    this.scalingParameters[i] = candScalingParameter;
                    break;
                }
            }
        }
        else
        {
            //scaling parameter is not updated
            str.append("unchanged");
        }

        str.append(", data size: ").append(FBUtilities.prettyPrintMemory(targetSize));
        str.append(", query cost: ").append(cost);
        str.append(", new query cost: ").append(candCost);
        str.append(", took ").append(TimeUnit.NANOSECONDS.toMicros(clock.now() - now)).append(" us");

        logger.debug(str.toString());
    }

    @Override
    public String toString()
    {
        return String.format("m: %d, o: %s, scalingParameter: %s - %s", minSstableSizeMB, Arrays.toString(survivalFactors), Arrays.toString(scalingParameters), calculator);
    }
}
