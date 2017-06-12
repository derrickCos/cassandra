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

package org.apache.cassandra.io.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.epoll.AIOEpollFileChannel;
import io.netty.channel.epoll.EpollEventLoop;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;


/**
 * A proxy of a FileChannel that:
 *
 * - implements reference counting
 * - exports only thread safe FileChannel operations
 * - wraps IO exceptions into runtime exceptions
 *
 * Tested by RandomAccessReaderTest.
 */
public final class AsynchronousChannelProxy extends SharedCloseableImpl
{
    private static final Logger logger = LoggerFactory.getLogger(AsynchronousChannelProxy.class);
    private final String filePath;
    private final AsynchronousFileChannel channel;

    public static AsynchronousFileChannel openFileChannel(File file, boolean mmapped)
    {
        try
        {
            if (!TPC.USE_AIO || mmapped)
                return AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.READ);

            EpollEventLoop loop = (EpollEventLoop) TPC.bestTPCScheduler().getExecutor();

            return new AIOEpollFileChannel(file, loop);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public AsynchronousChannelProxy(String path, boolean mmapped)
    {
        this (new File(path), mmapped);
    }

    public AsynchronousChannelProxy(File file, boolean mmapped)
    {
        this(file.getPath(), openFileChannel(file, mmapped));
    }

    public AsynchronousChannelProxy(String filePath, AsynchronousFileChannel channel)
    {
        super(new Cleanup(filePath, channel));

        this.filePath = filePath;
        this.channel = channel;
    }

    public AsynchronousChannelProxy(AsynchronousChannelProxy copy)
    {
        super(copy);

        this.filePath = copy.filePath;
        this.channel = copy.channel;
    }

    private final static class Cleanup implements RefCounted.Tidy
    {
        final String filePath;
        final AsynchronousFileChannel channel;

        Cleanup(String filePath, AsynchronousFileChannel channel)
        {
            this.filePath = filePath;
            this.channel = channel;
        }

        public String name()
        {
            return filePath;
        }

        public void tidy()
        {
            try
            {
                channel.close();
            }
            catch (IOException e)
            {
                throw new FSReadError(e, filePath);
            }
        }
    }

    public AsynchronousChannelProxy sharedCopy()
    {
        return new AsynchronousChannelProxy(this);
    }

    public String filePath()
    {
        return filePath;
    }

    public int readBlocking(ByteBuffer buffer, long position)
    {
        try
        {
            return channel.read(buffer, position).get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    public void read(ByteBuffer dest, long offset, CompletionHandler<Integer, ByteBuffer> onComplete)
    {
        try
        {
            channel.read(dest, offset, dest, onComplete);
        }
        catch (Throwable t)
        {
            onComplete.failed(t, dest);
        }
    }

    public long size()
    {
        try
        {
            return channel.size();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    public ChannelProxy getBlockingChannel()
    {
        return new ChannelProxy(new File(filePath));
    }

    public long transferTo(long position, long count, WritableByteChannel target)
    {
        try(ChannelProxy cp = new ChannelProxy(new File(filePath)))
        {
            return cp.transferTo(position, count, target);
        }
    }

    public int getFileDescriptor()
    {
        return NativeLibrary.getfd(channel);
    }

    @Override
    public String toString()
    {
        return filePath();
    }
}