package com.rock.socket;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.IOUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * @author cuishilei
 * @date 2019/6/26
 */
public class SocketSource extends RichSourceFunction<String> {
	/**
	 * Default delay between successive connection attempts.
	 */
	private static final int DEFAULT_CONNECTION_RETRY_SLEEP = 500;

	/**
	 * Default connection timeout when connecting to the server socket (infinite).
	 */
	private static final int CONNECTION_TIMEOUT_TIME = 0;

	private final String hostname;
	private final int port;
	private final String delimiter;
	private final String encoding;
	private final long maxNumRetries;
	private final long delayBetweenRetries;

	private transient Socket currentSocket;

	private volatile boolean isRunning = true;

	public SocketSource(String hostName, int port, String lineDelimiter, String encoding) {
		this(hostName, port, lineDelimiter, encoding, 0, DEFAULT_CONNECTION_RETRY_SLEEP);
	}

	private SocketSource(String hostName, int port, String lineDelimiter, String encoding, long maxNumRetries, long delayBetweenRetries) {
		this.hostname = hostName;
		this.port = port;
		this.delimiter = lineDelimiter;
		this.encoding = encoding;
		this.maxNumRetries = maxNumRetries;
		this.delayBetweenRetries = delayBetweenRetries;
	}

	@Override
	public void run(SourceContext<String> sourceContext) throws Exception {
		final StringBuilder buffer = new StringBuilder();
		long attempt = 0;

		while (isRunning) {

			try (Socket socket = new Socket()) {
				currentSocket = socket;

				socket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);
				try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), encoding))) {

					char[] cbuf = new char[8192];
					int bytesRead;
					while (isRunning && (bytesRead = reader.read(cbuf)) != -1) {
						buffer.append(cbuf, 0, bytesRead);
						int delimPos;
						while (buffer.length() >= delimiter.length() && (delimPos = buffer.indexOf(delimiter)) != -1) {
							String record = buffer.substring(0, delimPos);
							// truncate trailing carriage return
							if (delimiter.equals("\n") && record.endsWith("\r")) {
								record = record.substring(0, record.length() - 1);
							}
							sourceContext.collect(record);
							buffer.delete(0, delimPos + delimiter.length());
						}
					}
				}
			}

			// if we dropped out of this loop due to an EOF, sleep and retry
			if (isRunning) {
				attempt++;
				if (maxNumRetries == -1 || attempt < maxNumRetries) {
					Thread.sleep(delayBetweenRetries);
				} else {
					// this should probably be here, but some examples expect simple exists of the stream source
					// throw new EOFException("Reached end of stream and reconnects are not enabled.");
					break;
				}
			}
		}

		// collect trailing data
		if (buffer.length() > 0) {
			sourceContext.collect(buffer.toString());
		}
	}

	@Override
	public void cancel() {
		isRunning = false;

		// we need to close the socket as well, because the Thread.interrupt() function will
		// not wake the thread in the socketStream.read() method when blocked.
		Socket theSocket = this.currentSocket;
		if (theSocket != null) {
			IOUtils.closeSocket(theSocket);
		}
	}
}
