package com.basho.riak.pbc;

import java.io.IOException;
import java.net.InetAddress;

public class RiakConnectionFactory {

    private static final int CONNECTION_ACQUIRE_ATTEMPTS = 3;

    private final InetAddress host;
    private final int port;
    private final int bufferSizeKb;
    private final long connectionWaitTimeoutMillis;
    private final int requestTimeoutMillis;

    public RiakConnectionFactory(
        final InetAddress host,
        final int port,
        final int bufferSizeKb,
        final long connectionWaitTimeoutMillis,
        final int requestTimeoutMillis
    ) {
        this.host = host;
        this.port = port;
        this.bufferSizeKb = bufferSizeKb;
        this.connectionWaitTimeoutMillis = connectionWaitTimeoutMillis;
        this.requestTimeoutMillis = requestTimeoutMillis;
    }

    public RiakConnection createConnection(final RiakConnectionPool pool) throws IOException {
        return createConnection(pool, CONNECTION_ACQUIRE_ATTEMPTS);
    }

    public RiakConnection createConnection(final RiakConnectionPool pool, int attempts) throws IOException {
        try {
            return new RiakConnection(host, port, bufferSizeKb, pool, connectionWaitTimeoutMillis, requestTimeoutMillis);
        } catch (IOException ioe) {
            if (attempts > 0) {
                return createConnection(pool, attempts - 1);
            } else {
                throw ioe;
            }
        }
    }

    public InetAddress getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getBufferSizeKb() {
        return bufferSizeKb;
    }

    public long getConnectionWaitTimeoutMillis() {
        return connectionWaitTimeoutMillis;
    }

    public int getRequestTimeoutMillis() {
        return requestTimeoutMillis;
    }
}