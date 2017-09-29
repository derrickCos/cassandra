/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.nodesync;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Longs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.units.TimeValue;

/**
 * A {@link ValidationProposer} that forces the validation of particular range(s) of a particular table.
 * <p>
 * This proposer is not automatically created, it is only created by users (through JMX). It takes as parameter on
 * creation the table on which to operate and a list of (locally own) ranges to validate. It will then generate proposals
 * to validate <b>once</b> the requested ranges (or more specifically, of all segments necessary to fully cover the
 * requested ranges).
 * <p>
 * When using this proposer, the following specific properties should be kept in mind:
 *  - it doesn't check if the table has NodeSync enabled or not; it runs on any table irrespective of that setting (very
 *    much on purpose: this makes targeted testing of NodeSync (by us or by user) much easier). Note that  if the NodeSync
 *  - it uses the same segments for a table than {@link ContinuousTableValidationProposer}, and saves each segment
 *    validation results in the {@link SystemDistributedKeyspace#NODESYNC_STATUS} table (no reason to throw hard work
 *    on the floor). As the requested ranges may not fall exactly on segments boundary however, it means this may end up
 *    repairing a little bit more than strictly requested on both side of the range (hardly an issue in practice,
 *    especially since this will most likely be used either on all ranges, at the vnodes level or, from tooling like
 *    opsCenter, directly on segments).
 *  - it unconditionally validates all segments, meaning that while it saves results to {@link SystemDistributedKeyspace#NODESYNC_STATUS},
 *    it does not read from it and may well validate a segment that has been validated mere seconds ago.
 *  - it validate each segments it needs to cover exactly once, irrespective of the result of that validation. This
 *    means that some or all segments may well be only partially validated or even fail (the proposer do track and
 *    expose general metrics on its work). It is up to the user (or rather, external tooling) to re-trigger new forced
 *    validations if necessary.
 *  - validations generated by this proposer have strict precedences over any validation from
 *    {@link ContinuousTableValidationProposer}. This mean that, from a user point of view, a triggered validation
 *    basically "pause" normal NodeSync execution while it runs. If multiple {@link UserValidationProposer} are
 *    created concurrently, whichever one was created first will have priority over the other until full completion.
 */
public class UserValidationProposer extends AbstractValidationProposer
{
    private static final Logger logger = LoggerFactory.getLogger(UserValidationProposer.class);

    // An identifier of the user validation; see UserValidationOptions for details.
    private final String id;
    private final long createdTime = System.currentTimeMillis();

    // The list of ranges validated by this proposer. This contains only local, "normalized" ranges (in the sense of Range#normalize).
    // This can null to signify that all local ranges are requested.
    @Nullable
    private final ImmutableList<Range<Token>> validatedRanges;
    // The segments to validate
    private volatile List<Segment> toValidate;
    private final AtomicInteger nextIdx = new AtomicInteger(); // Atomic is tad overkill since supplyNextProposal is not
                                                               // supposed to be called concurrently but why risk any
                                                               // future problem, it's not performance sensitive.
    private final Future completionFuture = new Future();

    private final AtomicInteger remaining = new AtomicInteger();

    private volatile long startTime = -1L;
    private final AtomicIntegerArray outcomes = new AtomicIntegerArray(ValidationOutcome.values().length);
    private final AtomicReference<ValidationMetrics> metrics = new AtomicReference<>();

    private UserValidationProposer(NodeSyncService service,
                                   String id,
                                   TableMetadata table,
                                   int depth,
                                   ImmutableList<Range<Token>> validatedRanges)
    {
        super(service, table, depth);
        assert validatedRanges == null || !validatedRanges.isEmpty();
        this.id = id;
        this.validatedRanges = validatedRanges;
    }

    @VisibleForTesting
    List<Segment> segmentsToValidate()
    {
        return toValidate;
    }

    /**
     * The (externally provided) identifier for this validation proposer.
     */
    public String id()
    {
        return id;
    }

    /**
     * The (local) ranges that are validated by this proposer, or {@code null} if all local ranges are validated.
     */
    public @Nullable List<Range<Token>> validatedRanges()
    {
        return validatedRanges;
    }

    public void init()
    {
        if (toValidate != null)
            return; // Already initialized. Shouldn't happen but not much harm in being resilient to it.

        Collection<Range<Token>> localRanges = localRanges();
        generateSegments(localRanges);
        // We've validated all requested ranges are local and we have at least 1 (or we request all local ranges), so we
        // should have something to validate.
        assert !toValidate.isEmpty() : String.format("requested=%s, local=%s", validatedRanges, localRanges);
        nextIdx.set(0);
        remaining.set(toValidate.size());

        SystemDistributedKeyspace.recordNodeSyncUserValidation(this);
    }

    private void generateSegments(Collection<Range<Token>> localRanges)
    {
        // We generate all segments and filter the ones we care about. We could do a bit more efficient
        // algorithmically, especially when the ranges requested are small, but it's unlikely to matter in practice.
        List<Segment> allSegments = new ArrayList<>(Segments.estimateSegments(localRanges, depth));
        Iterators.addAll(allSegments, Segments.generateSegments(table, localRanges, depth));
        assert !allSegments.isEmpty();

        if (validatedRanges == null)
            toValidate = allSegments;
        else
            toValidate = allSegments.stream()
                                    .filter(s -> validatedRanges.stream().anyMatch(s.range::intersects))
                                    .collect(Collectors.toList());
    }

    static UserValidationProposer create(NodeSyncService service, UserValidationOptions options)
    {
        return create(service, options, NodeSyncService.SEGMENT_SIZE_TARGET);
    }

    @VisibleForTesting
    static UserValidationProposer create(NodeSyncService service,
                                         UserValidationOptions options,
                                         long maxSegmentSize)

    {
        TableMetadata table = options.table;
        ColumnFamilyStore store = ColumnFamilyStore.getIfExists(table.id);
        // We got the TableMetadata somewhere, so this suggests the table has been dropped concurrently from us
        if (store == null)
            throw new UnknownTableException(String.format("Cannot find table %s, it seems to have been dropped recently", table), table.id);

        int rf = store.keyspace.getReplicationStrategy().getReplicationFactor();
        if (rf <= 1)
            throw new IllegalArgumentException(String.format("Cannot do validation on table %s as it is not replicated "
                                                             + "(keyspace %s has replication factor %d)",
                                                             table, table.keyspace, rf));

        List<Range<Token>> local = Range.normalize(NodeSyncHelpers.localRanges(table.keyspace));
        List<Range<Token>> requested = options.validatedRanges;

        ImmutableList<Range<Token>> validated = requested == null ? null : ImmutableList.copyOf(requested);
        if (validated != null)
        {
            // We only validate local ranges so make sure to reject any non fully local requested range to avoid
            // later confusion (note that we deal with normalized ranges which makes this easier).
            checkAllLocalRanges(validated, local);
        }

        int depth = computeDepth(store, local.size(), maxSegmentSize);
        return new UserValidationProposer(service, options.id, table, depth, validated);
    }

    private static void checkAllLocalRanges(List<Range<Token>> validatedRanges, List<Range<Token>> localRanges)
    {
        // Because ranges are normalized, either every requested is strictly contained in one local range, or it
        // has some part that is not local.
        Set<Range<Token>> nonLocal = validatedRanges.stream()
                                                    .filter(r -> localRanges.stream().noneMatch(l -> l.contains(r)))
                                                    .collect(Collectors.toSet());
        if (!nonLocal.isEmpty())
            throw new IllegalArgumentException(String.format("Can only validate local ranges: ranges %s are not (entirely) local to node %s with ranges %s", nonLocal, FBUtilities.getBroadcastAddress(), localRanges));
    }

    /**
     * A future on the completion of the validation of this proposer.
     * <p>
     * Note that this future is completed when all the validation generated are completed, not when all the proposal
     * it generates are generated (which would be a lot less useful).
     *
     * @return a future on the completion of this proposer.
     */
    Future completionFuture()
    {
        return completionFuture;
    }

    public boolean supplyNextProposal(Consumer<ValidationProposal> proposalConsumer)
    {
        if (isDone() || nextIdx.get() >= toValidate.size())
            return false;

        proposalConsumer.accept(new Proposal(this, toValidate.get(nextIdx.getAndIncrement())));
        return true;
    }

    public boolean isDone()
    {
        return completionFuture.isDone();
    }

    public boolean cancel()
    {
        boolean cancelled = completionFuture.cancel(false);
        SystemDistributedKeyspace.recordNodeSyncUserValidation(this);
        return cancelled;
    }

    public boolean isCancelled()
    {
        return completionFuture.isCancelled();
    }

    private boolean isCompletedExceptionally()
    {
        return !isCancelled() && completionFuture.isCompletedExceptionally();
    }

    public Status status()
    {
        return Status.of(this);
    }

    public Statistics statistics()
    {
        return completionFuture.getCurrent();
    }

    public ValidationProposer onTableUpdate(TableMetadata table)
    {
        // As we ignore the nodesync 'enabled' setting, we don't care about any particular update
        return this;
    }

    private void onValidationDone(ValidationInfo info, ValidationMetrics metrics)
    {
        outcomes.incrementAndGet(info.outcome.ordinal());
        this.metrics.getAndAccumulate(metrics, (p, n) -> p == null ? n : ValidationMetrics.merge(p, n));

        if (remaining.decrementAndGet() == 0)
        {
            completionFuture.complete(new Statistics(startTime,
                                                     System.currentTimeMillis(),
                                                     this.metrics.get(),
                                                     extractOutcomes(),
                                                     toValidate.size()));
        }
        else
        {
            completionFuture.signalListeners();
        }

        SystemDistributedKeyspace.recordNodeSyncUserValidation(this);
    }

    private void onValidationError(Throwable error)
    {
        // The only validation "error" we get can is a CancellationException as Validator never explicitly call
        // completeExceptionally on its completion future (it logs and mark the segment failed instead). But even
        // if this changes, the right thing to do is to pass the error back to this proposer future.
        completionFuture.completeExceptionally(error);
        SystemDistributedKeyspace.recordNodeSyncUserValidation(this);
    }

    private long[] extractOutcomes()
    {
        long[] a = new long[outcomes.length()];
        for (int i = 0; i < outcomes.length(); i++)
            a[i] = outcomes.get(i);
        return a;
    }

    private static class Proposal extends ValidationProposal
    {
        private Proposal(UserValidationProposer proposer, Segment segment)
        {
            super(proposer, segment, HIGH_PRIORITY_LEVEL);
        }

        @Override
        UserValidationProposer proposer()
        {
            return (UserValidationProposer) super.proposer();
        }

        Validator activate()
        {
            if (proposer().isCancelled())
                return null;

            // We want to mark the 'startTime' (used for the stats returned on completion to tell how long the whole
            // thing took) to be set as close to the true beginning of the work as possible, so set it here. Note that
            // this is racy but we don't care.
            if (proposer().startTime < 0)
                proposer().startTime = System.currentTimeMillis();

            logger.trace("Submitting user validation of {} for execution", segment);
            Validator validator = Validator.createAndLock(proposer().service(), segment);
            validator.completionFuture()
                     .thenAccept(i -> proposer().onValidationDone(i, validator.metrics()))
                     .exceptionally(e -> { proposer().onValidationError(e); return null; });
            return validator;
        }

        @Override
        public int compareTo(ValidationProposal other)
        {
            if (!(other instanceof Proposal))
                return super.compareTo(other);

            Proposal that = (Proposal) other;
            assert this.priorityLevel == that.priorityLevel : this.priorityLevel + " != " + that.priorityLevel;

            // As described in the javadoc, if we have multiple user triggered proposer, the one created first goes
            // first for all proposals (no very strong reason for that btw, it just feels like the easiest to reason
            // with).
            // Same if by some uncanny coincidence 2 proposers are created at exactly the same millisecond.
            return Longs.compare(this.proposer().createdTime, that.proposer().createdTime);
        }

        @Override
        public String toString()
        {
            return String.format("%s(user triggered #%s @ %d)", segment, proposer().id, creationTime);
        }
    }

    /**
     * A future on the completion of a particular {@link UserValidationProposer}.
     * <p>
     * Note that on top of being a future on the completion of the proposer, this object also allow to monitor the
     * progress of said proposer.
     * TODO(Sylvain): the progress reporting part exists for the sake of providing JMX notifications but that is TBD.
     */
    public class Future extends CompletableFuture<Statistics>
    {
        private final List<Listener> listeners = new CopyOnWriteArrayList<>();
        private volatile Executor listenerExecutor;

        private Future()
        {
        }

        public UserValidationProposer proposer()
        {
            return UserValidationProposer.this;
        }

        /**
         * Get statistics on the work already done by this proposer. If the proposer completed successfully, this will
         * return the same object than {@link #get()} would, otherwise it returns statistics corresponding to work
         * already done by the proposer at the time of this call.
         */
        public Statistics getCurrent()
        {
            try
            {
                if (isDone() && !isCompletedExceptionally())
                    return get();

                return new Statistics(startTime, -1, metrics.get(), extractOutcomes(), toValidate.size());
            }
            catch (InterruptedException | ExecutionException e)
            {
                // We only call get() after having checked we cannot get any of those
                throw new AssertionError(e);
            }
        }

        /**
         * Register a listener that is fed statistics on the current status of this proposer based on the progress of
         * the validations involved.
         * <p>
         * How often the listener is called is influenced by {@code frequency} but it is always at least called:
         *  - with the current statistics (as of the call of this method).
         *  - with the final statistics for this proposer if the proposer completes successfully (and isn't done at
         *    the time of this call, since in that case the previous rule already takes care of the notification)
         * <p>
         * Note that listeners are called on a single threaded executor so it is guaranteed that {@code listener} calls
         * are never concurrent.
         *
         * @param listener the listener to be notified of the proposer progress.
         * @param frequency influences how often the listener is notified as a fraction of the percentage of progress.
         *                  What this mean is that if {@code frequency == 10} for instance, then the listener is called
         *                  for every 10% of progress. If this is 0 or a negative value, then the listener is called
         *                  after every segment validation.
         */
        public void registerProgressListener(Consumer<Statistics> listener, int frequency)
        {
            if (listenerExecutor == null)
                listenerExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("UserValidationEventExecutor"));

            // If not done, feed the current value. Racy but just a convenience:  getting 2 notifications is fine.
            if (!isDone())
                listener.accept(getCurrent());

            listeners.add(new Listener(listener, frequency));
            // Deal with the final notification use the future itself so we're sure we get the last notification even
            // if we happen to register after the proposer is done.
            thenAcceptAsync(listener, listenerExecutor);
        }

        private void signalListeners()
        {
            if (listenerExecutor == null)
                return;

            Statistics current = getCurrent();
            listenerExecutor.execute(() -> listeners.forEach(l -> {
                if (l.frequency <= 0 || current.progress() % l.frequency == 0)
                    l.consumer.accept(current);
            }));
        }
    }

    private static class Listener
    {
        private final Consumer<Statistics> consumer;
        private final int frequency;

        private Listener(Consumer<Statistics> consumer, int frequency)
        {
            this.consumer = consumer;
            this.frequency = frequency;
        }
    }

    /**
     * Statistics on the validations performed by a given user triggered validation proposer.
     */
    public static class Statistics
    {
        private final long startTime;
        private final long endTime;
        private final long currentTime;
        private final ValidationMetrics metrics;
        private final long[] outcomes;
        private final long segmentsToValidate;

        private Statistics(long startTime,
                           long endTime,
                           ValidationMetrics metrics,
                           long[] outcomes,
                           int segmentsToValidate)
        {
            this.startTime = startTime;
            this.endTime = endTime;
            this.currentTime = endTime < 0 ? System.currentTimeMillis() : endTime;
            this.metrics = metrics;
            this.outcomes = outcomes;
            this.segmentsToValidate = segmentsToValidate;
        }

        /** The time (in milliseconds) in which the proposer started to run its validation, {@code -1} if not started */
        public long startTime()
        {
            return startTime;
        }

        /** The time (in milliseconds) in which the proposer ended to run its validation, {@code -1} if not ended */
        public long endTime()
        {
            return endTime;
        }

        /**
         * The amount of time the proposer has been running its validation at the time this object has been created.
         * If this object represents statistics on a completed proposer, this is its total time of execution.
         */
        public TimeValue duration()
        {
            if (startTime < 0)
                return TimeValue.ZERO;

            long durationNanos = Math.max(currentTime - startTime, 0);
            return TimeValue.of(durationNanos, TimeUnit.MILLISECONDS);
        }

        /**
         * Whether all the validations reported by this objects (which, if this is statistics on a completed proposer,
         * represent all the validations) have been successful (in the sense of {@link ValidationOutcome#wasSuccessful()}).
         */
        public boolean wasFullySuccessful()
        {
            for (ValidationOutcome outcome : ValidationOutcome.values())
            {
                if (!outcome.wasSuccessful() && outcomes[outcome.ordinal()] != 0)
                    return false;
            }
            return true;
        }

        /**
         * The number of segments that have been validated at the time those statistics have been created. If this is the
         * statistics of a (successfully) completed proposer, then {@code segmentsValidated() == segmentsToValidate()}.
         */
        public long segmentValidated()
        {
            return LongStream.of(outcomes).sum();
        }

        /**
         * The total number of segments that the proposer this is the statistics of has to validate (which may be more
         * that the number of segments this object reports about if the proposer is not complete).
         */
        public long segmentsToValidate()
        {
            return segmentsToValidate;
        }

        public long[] getOutcomes()
        {
            return outcomes;
        }

        /**
         * The number of segments (reported by this object) whose validation ended up with the provided outcome.
         *
         * @param outcome the outcome to check.
         * @return the number of segments validated by the proposer this is the statistics of that had the outcome
         * represented by {@code outcome}.
         */
        public long numberOfSegmentsWithOutcome(ValidationOutcome outcome)
        {
            return outcomes[outcome.ordinal()];
        }

        /**
         * The progress (as a percentage) of the proposer this is the statistics of that this object represents.
         *
         * @return a value between 0 and 100 that represents the percentage of segments validated this object represents
         * over the total amount of segments the proposer has to validate.
         */
        public int progress()
        {
            int p = (int)(((double)segmentValidated() / segmentsToValidate()) * 100d);
            return Math.max(0, Math.min(100, p));
        }

        /**
         * Metrics on the validations this is a statistics of.
         */
        @Nullable
        public ValidationMetrics metrics()
        {
            return metrics;
        }
    }

    /**
     * Represents the completion status of the validation.
     */
    public enum Status
    {
        RUNNING,
        SUCCESSFUL,
        CANCELLED,
        FAILED;

        private static Status of(UserValidationProposer proposer)
        {
            if (proposer.isCancelled())
                return CANCELLED;

            if (proposer.isCompletedExceptionally())
                return FAILED;

            if (proposer.isDone())
                return SUCCESSFUL;

            return RUNNING;
        }

        public Status combineWith(Status other)
        {
            if (this == RUNNING || other == RUNNING)
                return RUNNING;

            if (this == FAILED || other == FAILED)
                return FAILED;

            if (this == CANCELLED || other == CANCELLED)
                return CANCELLED;

            return other;
        }

        public static Status from(String s)
        {
            return valueOf(s.toUpperCase());
        }

        public String toString()
        {
            return super.toString().toLowerCase();
        }
    }

}
