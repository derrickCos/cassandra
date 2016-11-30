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
package org.apache.cassandra.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueue;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.subjects.PublishSubject;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.transport.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.cassandra.concurrent.NettyRxScheduler;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * A message from the CQL binary protocol.
 */
public abstract class Message
{
    protected static final Logger logger = LoggerFactory.getLogger(Message.class);

    public final static FastThreadLocal<PublishSubject<RequestContext>> localPublishSubject = new FastThreadLocal<PublishSubject<RequestContext>>(){

        protected PublishSubject<RequestContext> initialValue()
        {
            return PublishSubject.create();
        }
    };

    // TODO we probably don't actually need any of this part of the chain at all, only the response chain...
    public static void buildChain(Observable<RequestContext> observable)
    {
        observable.subscribe(
                // onNext
                requestContext -> {
                    requestContext.request.executePipeline(requestContext);
                },

                // onError
                exc -> {
                    throw new UnsupportedOperationException("Can't handle onError yet");
                },

                () -> {
                    throw new UnsupportedOperationException("Can't handle onComplete yet");
                });
        /*
        observable.map(requestContext -> {
            requestContext.request.executePipeline(requestContext);
            return requestContext;
        }).subscribe();
        */
    }

    public static void subscribeChain(Observable<RequestContext> observable)
    {
        observable.subscribe(

                // onNext
                requestContext -> {

                    Message.Response response = requestContext.response;
                    Message.Request request = requestContext.request;
                    response.setStreamId(requestContext.request.getStreamId());

                    // TODO odds are low of this working correctly
                    response.setWarnings(ClientWarn.instance.getWarnings());

                    ((ServerConnection) request.connection).applyStateTransition(request.type, response.type);

                    logger.trace("Responding: {}, v={} ON {}", response, request.connection.getVersion(), Thread.currentThread().getName());
                    requestContext.dispatcher.flush(requestContext);
                    ClientWarn.instance.resetWarnings();
                },

                // onError
                exc ->  {
                    throw new UnsupportedOperationException("Can't handle onError in chain yet");
                },

                // onComplete
                () -> {
                    throw new UnsupportedOperationException("Can't handle onComplete in chain yet");
                }
        );
    }

    /**
     * When we encounter an unexpected IOException we look for these {@link Throwable#getMessage() messages}
     * (because we have no better way to distinguish) and log them at DEBUG rather than INFO, since they
     * are generally caused by unclean client disconnects rather than an actual problem.
     */
    private static final Set<String> ioExceptionsAtDebugLevel = ImmutableSet.<String>builder().
            add("Connection reset by peer").
            add("Broken pipe").
            add("Connection timed out").
            build();

    public interface Codec<M extends Message> extends CBCodec<M> {}

    public enum Direction
    {
        REQUEST, RESPONSE;

        public static Direction extractFromVersion(int versionWithDirection)
        {
            return (versionWithDirection & 0x80) == 0 ? REQUEST : RESPONSE;
        }

        public int addToVersion(int rawVersion)
        {
            return this == REQUEST ? (rawVersion & 0x7F) : (rawVersion | 0x80);
        }
    }

    public enum Type
    {
        ERROR          (0,  Direction.RESPONSE, ErrorMessage.codec),
        STARTUP        (1,  Direction.REQUEST,  StartupMessage.codec),
        READY          (2,  Direction.RESPONSE, ReadyMessage.codec),
        AUTHENTICATE   (3,  Direction.RESPONSE, AuthenticateMessage.codec),
        CREDENTIALS    (4,  Direction.REQUEST,  CredentialsMessage.codec),
        OPTIONS        (5,  Direction.REQUEST,  OptionsMessage.codec),
        SUPPORTED      (6,  Direction.RESPONSE, SupportedMessage.codec),
        QUERY          (7,  Direction.REQUEST,  QueryMessage.codec),
        RESULT         (8,  Direction.RESPONSE, ResultMessage.codec),
        PREPARE        (9,  Direction.REQUEST,  PrepareMessage.codec),
        EXECUTE        (10, Direction.REQUEST,  ExecuteMessage.codec),
        REGISTER       (11, Direction.REQUEST,  RegisterMessage.codec),
        EVENT          (12, Direction.RESPONSE, EventMessage.codec),
        BATCH          (13, Direction.REQUEST,  BatchMessage.codec),
        AUTH_CHALLENGE (14, Direction.RESPONSE, AuthChallenge.codec),
        AUTH_RESPONSE  (15, Direction.REQUEST,  AuthResponse.codec),
        AUTH_SUCCESS   (16, Direction.RESPONSE, AuthSuccess.codec);

        public final int opcode;
        public final Direction direction;
        public final Codec<?> codec;

        private static final Type[] opcodeIdx;
        static
        {
            int maxOpcode = -1;
            for (Type type : Type.values())
                maxOpcode = Math.max(maxOpcode, type.opcode);
            opcodeIdx = new Type[maxOpcode + 1];
            for (Type type : Type.values())
            {
                if (opcodeIdx[type.opcode] != null)
                    throw new IllegalStateException("Duplicate opcode");
                opcodeIdx[type.opcode] = type;
            }
        }

        private Type(int opcode, Direction direction, Codec<?> codec)
        {
            this.opcode = opcode;
            this.direction = direction;
            this.codec = codec;
        }

        public static Type fromOpcode(int opcode, Direction direction)
        {
            if (opcode >= opcodeIdx.length)
                throw new ProtocolException(String.format("Unknown opcode %d", opcode));
            Type t = opcodeIdx[opcode];
            if (t == null)
                throw new ProtocolException(String.format("Unknown opcode %d", opcode));
            if (t.direction != direction)
                throw new ProtocolException(String.format("Wrong protocol direction (expected %s, got %s) for opcode %d (%s)",
                                                          t.direction,
                                                          direction,
                                                          opcode,
                                                          t));
            return t;
        }
    }

    public final Type type;
    protected Connection connection;
    private int streamId;
    private Frame sourceFrame;
    private Map<String, ByteBuffer> customPayload;
    protected Integer forcedProtocolVersion = null;

    protected Message(Type type)
    {
        this.type = type;
    }

    public void attach(Connection connection)
    {
        this.connection = connection;
    }

    public Connection connection()
    {
        return connection;
    }

    public Message setStreamId(int streamId)
    {
        this.streamId = streamId;
        return this;
    }

    public int getStreamId()
    {
        return streamId;
    }

    public void setSourceFrame(Frame sourceFrame)
    {
        this.sourceFrame = sourceFrame;
    }

    public Frame getSourceFrame()
    {
        return sourceFrame;
    }

    public Map<String, ByteBuffer> getCustomPayload()
    {
        return customPayload;
    }

    public void setCustomPayload(Map<String, ByteBuffer> customPayload)
    {
        this.customPayload = customPayload;
    }

    public static abstract class Request extends Message
    {
        protected boolean tracingRequested;

        protected Request(Type type)
        {
            super(type);

            if (type.direction != Direction.REQUEST)
                throw new IllegalArgumentException();
        }

        public abstract Single<? extends Response> execute(QueryState queryState, long queryStartNanoTime);

        public void setTracingRequested()
        {
            this.tracingRequested = true;
        }

        public boolean isTracingRequested()
        {
            return tracingRequested;
        }

        public boolean supportsPipelineExecution()
        {
            return false;
        }

        public void executePipeline(RequestContext requestContext)
        {
            throw new UnsupportedOperationException("executePipeline() not supported for this class: " + this.getClass().getName());
        }
    }

    public static abstract class Response extends Message
    {
        protected UUID tracingId;
        protected List<String> warnings;

        protected Response(Type type)
        {
            super(type);

            if (type.direction != Direction.RESPONSE)
                throw new IllegalArgumentException();
        }

        public Message setTracingId(UUID tracingId)
        {
            this.tracingId = tracingId;
            return this;
        }

        public UUID getTracingId()
        {
            return tracingId;
        }

        public Message setWarnings(List<String> warnings)
        {
            this.warnings = warnings;
            return this;
        }

        public List<String> getWarnings()
        {
            return warnings;
        }
    }

    @ChannelHandler.Sharable
    public static class ProtocolDecoder extends MessageToMessageDecoder<Frame>
    {
        public void decode(ChannelHandlerContext ctx, Frame frame, List results)
        {
            boolean isRequest = frame.header.type.direction == Direction.REQUEST;
            boolean isTracing = frame.header.flags.contains(Frame.Header.Flag.TRACING);
            boolean isCustomPayload = frame.header.flags.contains(Frame.Header.Flag.CUSTOM_PAYLOAD);
            boolean hasWarning = frame.header.flags.contains(Frame.Header.Flag.WARNING);

            UUID tracingId = isRequest || !isTracing ? null : CBUtil.readUUID(frame.body);
            List<String> warnings = isRequest || !hasWarning ? null : CBUtil.readStringList(frame.body);
            Map<String, ByteBuffer> customPayload = !isCustomPayload ? null : CBUtil.readBytesMap(frame.body);

            try
            {
                if (isCustomPayload && frame.header.version < Server.VERSION_4)
                    throw new ProtocolException("Received frame with CUSTOM_PAYLOAD flag for native protocol version < 4");

                Message message = frame.header.type.codec.decode(frame.body, frame.header.version);
                message.setStreamId(frame.header.streamId);
                message.setSourceFrame(frame);
                message.setCustomPayload(customPayload);

                if (isRequest)
                {
                    assert message instanceof Request;
                    Request req = (Request)message;
                    Connection connection = ctx.channel().attr(Connection.attributeKey).get();
                    req.attach(connection);
                    if (isTracing)
                        req.setTracingRequested();
                }
                else
                {
                    assert message instanceof Response;
                    if (isTracing)
                        ((Response)message).setTracingId(tracingId);
                    if (hasWarning)
                        ((Response)message).setWarnings(warnings);
                }

                results.add(message);
            }
            catch (Throwable ex)
            {
                frame.release();
                // Remember the streamId
                throw ErrorMessage.wrap(ex, frame.header.streamId);
            }
        }
    }

    @ChannelHandler.Sharable
    public static class ProtocolEncoder extends MessageToMessageEncoder<Message>
    {
        public void encode(ChannelHandlerContext ctx, Message message, List results)
        {
            Connection connection = ctx.channel().attr(Connection.attributeKey).get();
            // The only case the connection can be null is when we send the initial STARTUP message (client side thus)
            int version = connection == null ? Server.CURRENT_VERSION : connection.getVersion();

            EnumSet<Frame.Header.Flag> flags = EnumSet.noneOf(Frame.Header.Flag.class);
            if (version == Server.BETA_VERSION)
                flags.add(Frame.Header.Flag.USE_BETA);

            Codec<Message> codec = (Codec<Message>)message.type.codec;
            try
            {
                int messageSize = codec.encodedSize(message, version);
                ByteBuf body;
                if (message instanceof Response)
                {
                    UUID tracingId = ((Response)message).getTracingId();
                    Map<String, ByteBuffer> customPayload = message.getCustomPayload();
                    if (tracingId != null)
                        messageSize += CBUtil.sizeOfUUID(tracingId);
                    List<String> warnings = ((Response)message).getWarnings();
                    if (warnings != null)
                    {
                        if (version < Server.VERSION_4)
                            throw new ProtocolException("Must not send frame with WARNING flag for native protocol version < 4");
                        messageSize += CBUtil.sizeOfStringList(warnings);
                    }
                    if (customPayload != null)
                    {
                        if (version < Server.VERSION_4)
                            throw new ProtocolException("Must not send frame with CUSTOM_PAYLOAD flag for native protocol version < 4");
                        messageSize += CBUtil.sizeOfBytesMap(customPayload);
                    }
                    body = CBUtil.allocator.buffer(messageSize);
                    if (tracingId != null)
                    {
                        CBUtil.writeUUID(tracingId, body);
                        flags.add(Frame.Header.Flag.TRACING);
                    }
                    if (warnings != null)
                    {
                        CBUtil.writeStringList(warnings, body);
                        flags.add(Frame.Header.Flag.WARNING);
                    }
                    if (customPayload != null)
                    {
                        CBUtil.writeBytesMap(customPayload, body);
                        flags.add(Frame.Header.Flag.CUSTOM_PAYLOAD);
                    }
                }
                else
                {
                    assert message instanceof Request;
                    if (((Request)message).isTracingRequested())
                        flags.add(Frame.Header.Flag.TRACING);
                    Map<String, ByteBuffer> payload = message.getCustomPayload();
                    if (payload != null)
                        messageSize += CBUtil.sizeOfBytesMap(payload);
                    body = CBUtil.allocator.buffer(messageSize);
                    if (payload != null)
                    {
                        CBUtil.writeBytesMap(payload, body);
                        flags.add(Frame.Header.Flag.CUSTOM_PAYLOAD);
                    }
                }

                try
                {
                    codec.encode(message, body, version);
                }
                catch (Throwable e)
                {
                    body.release();
                    throw e;
                }

                // if the driver attempted to connect with a protocol version lower than the minimum supported
                // version, respond with a protocol error message with the correct frame header for that version
                int responseVersion = message.forcedProtocolVersion == null
                                    ? version
                                    : message.forcedProtocolVersion;
                results.add(Frame.create(message.type, message.getStreamId(), responseVersion, flags, body));
            }
            catch (Throwable e)
            {
                throw ErrorMessage.wrap(e, message.getStreamId());
            }
        }
    }

    @ChannelHandler.Sharable
    public static class Dispatcher extends SimpleChannelInboundHandler<Request>
    {
        private static class FlushItem
        {
            final ChannelHandlerContext ctx;
            final Object response;
            final Frame sourceFrame;
            private FlushItem(ChannelHandlerContext ctx, Object response, Frame sourceFrame)
            {
                this.ctx = ctx;
                this.sourceFrame = sourceFrame;
                this.response = response;
            }
        }

        private static class ChannelFlusher
        {
            final ChannelHandlerContext ctx;
            final List<FlushItem> flushItems = new ArrayList<>();
            int runsSinceFlush = 0;

            ChannelFlusher(ChannelHandlerContext ctx)
            {
                this.ctx = ctx;
            }

            void add(FlushItem item)
            {
                ctx.write(item.response, ctx.voidPromise());
                flushItems.add(item);
            }

            boolean maybeFlush()
            {
                if (runsSinceFlush > 2 || flushItems.size() > 50)
                {
                    ctx.flush();
                    for (FlushItem item : flushItems)
                        item.sourceFrame.release();

                    flushItems.clear();

                    runsSinceFlush = 0;
                    return true;
                }

                runsSinceFlush++;
                return false;
            }

        }

        private static final class Flusher implements Runnable
        {
            final EventLoop eventLoop;
            final MpscArrayQueue<FlushItem> queued = new MpscArrayQueue<>(1 << 16);
            final AtomicBoolean running = new AtomicBoolean(false);
            final Map<ChannelHandlerContext, ChannelFlusher> channels = new IdentityHashMap<>();
            final List<ChannelHandlerContext> finishedChannels = new ArrayList<>();
            int runsWithNoWork = 0;
            private Flusher(EventLoop eventLoop)
            {
                this.eventLoop = eventLoop;
            }
            void start()
            {
                if (!running.get() && running.compareAndSet(false, true))
                    this.eventLoop.execute(this);
            }
            public void run()
            {

                boolean doneWork = false;
                FlushItem item;
                while ( null != (item = queued.poll()) )
                {
                    channels.computeIfAbsent(item.ctx, ChannelFlusher::new).add(item);
                    doneWork = true;
                }

                for (Map.Entry<ChannelHandlerContext, ChannelFlusher> c : channels.entrySet())
                {
                    if (c.getKey().channel().isActive())
                        c.getValue().maybeFlush();
                    else
                        finishedChannels.add(c.getKey());
                }

                for (ChannelHandlerContext c : finishedChannels)
                    channels.remove(c);

                finishedChannels.clear();

                if (doneWork)
                {
                    runsWithNoWork = 0;
                }
                else
                {
                    // either reschedule or cancel
                    if (++runsWithNoWork > 5)
                    {
                        running.set(false);

                        if (queued.isEmpty())
                            return;

                        //Somebody already took over so we can exit
                        if (!running.compareAndSet(false, true))
                            return;
                    }
                }

                eventLoop.schedule(this, 10000, TimeUnit.NANOSECONDS);
            }
        }

        private static final ConcurrentMap<EventLoop, Flusher> flusherLookup = new ConcurrentHashMap<>();

        public Dispatcher()
        {
            super(false);
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, Request request)
        {
            final ServerConnection connection;
            long queryStartNanoTime = System.nanoTime();

            assert request.connection() instanceof ServerConnection;
            connection = (ServerConnection) request.connection();
            if (connection.getVersion() >= Server.VERSION_4)
                ClientWarn.instance.captureWarnings();

            QueryState qstate = connection.validateNewMessage(request.type, connection.getVersion(), request.getStreamId());
            logger.trace("Received: {}, v={} ON {}", request, connection.getVersion(), Thread.currentThread().getName());

            if (request.supportsPipelineExecution())
            {
                RequestContext requestContext = new RequestContext(request, qstate, queryStartNanoTime, this, ctx);
                requestContext.request.executePipeline(requestContext);
            }
            else
            {
                request.execute(qstate, queryStartNanoTime)

                        // TODO evaluate the performance impact of this, we shouldn't need it if the PPC requests are to the correct port
                        // .observeOn(NettyRxScheduler.instance())

                        .subscribe(
                                // onSuccess
                                response -> {
                                    response.setStreamId(request.getStreamId());

                                    response.setWarnings(ClientWarn.instance.getWarnings());
                                    response.attach(connection);
                                    connection.applyStateTransition(request.type, response.type);

                                    logger.trace("Responding: {}, v={} ON {}", response, connection.getVersion(), Thread.currentThread().getName());
                                    flush(new FlushItem(ctx, response, request.getSourceFrame()));
                                    ClientWarn.instance.resetWarnings();
                                },

                                // onError
                                t -> {
                                    JVMStabilityInspector.inspectThrowable(t);
                                    UnexpectedChannelExceptionHandler handler = new UnexpectedChannelExceptionHandler(ctx.channel(), true);
                                    Message response = ErrorMessage.fromException(t, handler).setStreamId(request.getStreamId());
                                    flush(new FlushItem(ctx, response, request.getSourceFrame()));
                                    ClientWarn.instance.resetWarnings();
                                }
                        );
            }
        }

        private void flush(RequestContext requestContext)
        {
            flush(new FlushItem(requestContext.channelHandlerContext, requestContext.response, requestContext.request.getSourceFrame()));
        }

        /** Aggregates writes from this event loop to be flushed at once
         * This method will only be called from the eventloop itself so is threadsafe
         * @param item
         */
        private void flush(FlushItem item)
        {
            EventLoop loop = item.ctx.channel().eventLoop();
            Flusher flusher = flusherLookup.get(loop);
            if (flusher == null)
            {
                Flusher alt = flusherLookup.putIfAbsent(loop, flusher = new Flusher(loop));
                if (alt != null)
                    flusher = alt;
            }

            flusher.queued.add(item);
            flusher.start();
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, Throwable cause)
        throws Exception
        {
            if (ctx.channel().isOpen())
            {
                UnexpectedChannelExceptionHandler handler = new UnexpectedChannelExceptionHandler(ctx.channel(), false);
                ChannelFuture future = ctx.writeAndFlush(ErrorMessage.fromException(cause, handler));
                // On protocol exception, close the channel as soon as the message have been sent
                if (cause instanceof ProtocolException)
                {
                    future.addListener(new ChannelFutureListener()
                    {
                        public void operationComplete(ChannelFuture future)
                        {
                            ctx.close();
                        }
                    });
                }
            }
        }
    }

    /**
     * Include the channel info in the logged information for unexpected errors, and (if {@link #alwaysLogAtError} is
     * false then choose the log level based on the type of exception (some are clearly client issues and shouldn't be
     * logged at server ERROR level)
     */
    static final class UnexpectedChannelExceptionHandler implements Predicate<Throwable>
    {
        private final Channel channel;
        private final boolean alwaysLogAtError;

        UnexpectedChannelExceptionHandler(Channel channel, boolean alwaysLogAtError)
        {
            this.channel = channel;
            this.alwaysLogAtError = alwaysLogAtError;
        }

        @Override
        public boolean apply(Throwable exception)
        {
            String message;
            try
            {
                message = "Unexpected exception during request; channel = " + channel;
            }
            catch (Exception ignore)
            {
                // We don't want to make things worse if String.valueOf() throws an exception
                message = "Unexpected exception during request; channel = <unprintable>";
            }

            if (!alwaysLogAtError && exception instanceof IOException)
            {
                if (ioExceptionsAtDebugLevel.contains(exception.getMessage()))
                {
                    // Likely unclean client disconnects
                    logger.trace(message, exception);
                }
                else
                {
                    // Generally unhandled IO exceptions are network issues, not actual ERRORS
                    logger.info(message, exception);
                }
            }
            else
            {
                // Anything else is probably a bug in server of client binary protocol handling
                logger.error(message, exception);
            }

            // We handled the exception.
            return true;
        }
    }
}
