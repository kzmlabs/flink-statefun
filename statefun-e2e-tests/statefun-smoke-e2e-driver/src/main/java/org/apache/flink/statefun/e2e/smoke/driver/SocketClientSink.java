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
package org.apache.flink.statefun.e2e.smoke.driver;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializableObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketClientSink<T> implements Sink<T> {
  private static final Logger LOG = LoggerFactory.getLogger(SocketClientSink.class);

  private static final long serialVersionUID = 1;
  private final String hostName;
  private final int port;
  private final SerializationSchema<T> schema;
  private final int maxNumRetries;
  private final boolean autoflush;

  public SocketClientSink(
      String hostName,
      int port,
      SerializationSchema<T> schema,
      int maxNumRetries,
      boolean autoflush) {
    Preconditions.checkArgument(NetUtils.isValidClientPort(port), "port is out of range");
    Preconditions.checkArgument(
        maxNumRetries >= -1,
        "maxNumRetries must be zero or larger (num retries), or -1 (infinite retries)");
    this.hostName = Preconditions.checkNotNull(hostName, "hostname must not be null");
    this.port = port;
    this.schema = Preconditions.checkNotNull(schema);
    this.maxNumRetries = maxNumRetries;
    this.autoflush = autoflush;
  }

  public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
    return new SocketClientSinkWriter(
        this.hostName, this.port, this.schema, this.maxNumRetries, this.autoflush);
  }

  private class SocketClientSinkWriter implements SinkWriter<T> {
    private final SerializableObject lock;
    private final String hostName;
    private final int port;
    private final SerializationSchema<T> schema;
    private final int maxNumRetries;
    private final boolean autoflush;
    private boolean isRunning;
    private Socket client;
    private OutputStream outputStream;

    public SocketClientSinkWriter(
        String hostName,
        int port,
        SerializationSchema<T> schema,
        int maxNumRetries,
        boolean autoflush)
        throws IOException {
      this.hostName = hostName;
      this.port = port;
      this.lock = new SerializableObject();
      this.isRunning = true;
      this.schema = schema;
      this.maxNumRetries = maxNumRetries;
      this.autoflush = autoflush;

      this.createConnection();
    }

    public void write(T value, SinkWriter.Context context)
        throws IOException, InterruptedException {
      byte[] msg = this.schema.serialize(value);
      try {
        this.outputStream.write(msg);
        if (this.autoflush) {
          this.outputStream.flush();
        }
      } catch (IOException e) {
        if (this.maxNumRetries == 0) {
          throw new IOException(
              "Failed to send message '"
                  + String.valueOf(value)
                  + "' to socket server at "
                  + this.hostName
                  + ":"
                  + this.port
                  + ". Connection re-tries are not enabled.",
              e);
        }

        LOG.error(
            "Failed to send message '"
                + String.valueOf(value)
                + "' to socket server at "
                + this.hostName
                + ":"
                + this.port
                + ". Trying to reconnect...",
            e);
        synchronized (this.lock) {
          IOException lastException = null;
          int retries = 0;

          while (this.isRunning && (this.maxNumRetries < 0 || retries < this.maxNumRetries)) {
            try {
              if (this.outputStream != null) {
                this.outputStream.close();
              }
            } catch (IOException ee) {
              LOG.error("Could not close output stream from failed write attempt", ee);
            }

            try {
              if (this.client != null) {
                this.client.close();
              }
            } catch (IOException ee) {
              LOG.error("Could not close socket from failed write attempt", ee);
            }

            ++retries;

            try {
              this.createConnection();
              this.outputStream.write(msg);
            } catch (IOException ee) {
              lastException = ee;
              LOG.error(
                  "Re-connect to socket server and send message failed. Retry time(s): " + retries,
                  ee);
              this.lock.wait(500L);
              continue;
            }

            return;
          }

          if (this.isRunning) {
            throw new IOException(
                "Failed to send message '"
                    + String.valueOf(value)
                    + "' to socket server at "
                    + this.hostName
                    + ":"
                    + this.port
                    + ". Failed after "
                    + retries
                    + " retries.",
                lastException);
          }
        }
      }
    }

    public void flush(boolean endOfInput) throws IOException {
      if (this.outputStream != null) {
        this.outputStream.flush();
      }
    }

    public void close() throws IOException {
      this.isRunning = false;
      synchronized (this.lock) {
        this.lock.notifyAll();
        try {
          if (this.outputStream != null) {
            this.outputStream.close();
          }
        } finally {
          this.outputStream = null;

          if (this.client != null) {
            try {
              this.client.close();
            } finally {
              this.client = null;
            }
          }
        }
      }
    }

    private void createConnection() throws IOException {
      this.client = new Socket(hostName, port);
      this.client.setKeepAlive(true);
      this.client.setTcpNoDelay(true);
      this.outputStream = this.client.getOutputStream();
    }
  }
}
