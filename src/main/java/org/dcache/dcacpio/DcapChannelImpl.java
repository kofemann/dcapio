package org.dcache.dcacpio;

import com.google.common.base.Stopwatch;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * DCAP based implementation of {@link ProxyIoAdapter}.
 */
public class DcapChannelImpl implements ProxyIoAdapter {

    /**
     * DCAP binary commands
     */
    private static final int CLOSE = 4;
    private static final int SEEK_AND_READ = 11;
    private static final int SEEK_AND_WRITE = 12;

    /**
     * Protocol constants
     */
    private static final int DATA = 8;
    private static final int EOD = -1;

    /**
     * DCAP SEEK whence
     */
    private static final int SEEK_SET = 0;

    private final long TOTAL_TIMEOUT = TimeUnit.SECONDS.toMillis(4);
    private final SocketChannel _channel;
    private final Selector _channelSelector;
    private final long _size;
    private final int _sessionId;

    private final ChannelHandler READ = new ChannelReadHandler();
    private final ChannelHandler WRITE = new ChannelWriteHandler();

    public DcapChannelImpl(InetSocketAddress addr, int session, byte[] challange, long size) throws IOException {
        _channel = SocketChannel.open(addr);
        _channel.configureBlocking(false);
	_channel.finishConnect();
	_channelSelector = Selector.open();
        ByteBuffer buf = ByteBuffer.allocate(8 + challange.length);
        buf.order(ByteOrder.BIG_ENDIAN);
        buf.putInt(session);
        buf.putInt(challange.length).put(challange);
        buf.flip();
        fullIo(_channel, buf, WRITE);
        _size = size;
        _sessionId = session;
    }

    @Override
    public int getSessionId() {
        return _sessionId;
    }

    @Override
    public synchronized int read(ByteBuffer dst, long position) throws IOException {

        ByteBuffer command = new DcapCommandBuilder()
                .withSeekAndRead(position, dst.remaining())
                .build();

        fullIo(_channel, command, WRITE);
        getAck();
        return getData(dst);
    }


    @Override
    public synchronized int write(ByteBuffer src, long position) throws IOException {
        ByteBuffer command = new DcapCommandBuilder()
                .withSeekAndWrite(position, src.remaining())
                .build();

        fullIo(_channel, command, WRITE);
        getAck();
        return sendData(src);
    }

    @Override
    public long size() {
        return _size;
    }

    @Override
    public synchronized void close() throws IOException {
        ByteBuffer command = new DcapCommandBuilder()
                .withClose()
                .build();

        fullIo(_channel, command, WRITE);
        getAck();
        _channel.close();
    }

    private int getData(ByteBuffer buf) throws IOException {
        ByteBuffer dataBlock = ByteBuffer.allocate(128);
        dataBlock.order(ByteOrder.BIG_ENDIAN);

        dataBlock.limit(8);
        fullIo(_channel, dataBlock, READ);
        int total = 0;
        while (true) {
            dataBlock.clear();
            dataBlock.limit(4);
            fullIo(_channel, dataBlock, READ);
            dataBlock.flip();
            int n = dataBlock.getInt();
            if (n < 0) {
                getAck();
                break;
            }

            ByteBuffer chunk = buf.slice();
            chunk.limit(n);
            fullIo(_channel, chunk, READ);
            buf.position(buf.position() + n);
            total += n;
        }
        return total;
    }

    private int sendData(ByteBuffer b) throws IOException {
        int nbytes = b.remaining();
        ByteBuffer dataBlock = ByteBuffer.allocate(12);
        dataBlock.order(ByteOrder.BIG_ENDIAN);
        dataBlock.putInt(4);
        dataBlock.putInt(DATA);
        dataBlock.putInt(nbytes);
        dataBlock.flip();

        fullIo(_channel, dataBlock, WRITE);
        fullIo(_channel, b, WRITE);
        dataBlock.clear();
        dataBlock.putInt(EOD);
        dataBlock.flip();
        fullIo(_channel, dataBlock, WRITE);
        getAck();
        return nbytes;
    }

    private void getAck() throws IOException {
        ByteBuffer ackBuffer = ByteBuffer.allocate(256);
        ackBuffer.order(ByteOrder.BIG_ENDIAN);
        ackBuffer.limit(4);
        fullIo(_channel, ackBuffer, READ);
        ackBuffer.flip();
        int len = ackBuffer.getInt();
        ackBuffer.clear().limit(len);
        fullIo(_channel, ackBuffer, READ);
        // FIXME: error handling
    }

    private void fullIo(SocketChannel channel, ByteBuffer buf, ChannelHandler channelHandler) throws IOException {
	Stopwatch sw = Stopwatch.createStarted();
	channel.register(_channelSelector, channelHandler.getInterest());
        while (buf.hasRemaining()) {
	    long timeToWait = TOTAL_TIMEOUT - sw.elapsed(TimeUnit.MILLISECONDS);
	    if (timeToWait <= 0) {
		throw new IOException("timeout");
	    }
	    int n = _channelSelector.select(timeToWait);
	    if (n > 0) {
		/*
		 * we have here a very simplified logic as there is only one thread and one socket.
		 */
		Iterator<SelectionKey> selectionKeyIterator = _channelSelector.selectedKeys().iterator();
		SelectionKey key = selectionKeyIterator.next();
		selectionKeyIterator.remove();
		if ((key.readyOps() & channelHandler.getInterest()) != 0) {
		    channelHandler.handleChalled(channel, buf);
		}
	    }
        }
    }

    private abstract static class ChannelHandler {

	protected abstract int getInterest();

	protected abstract void handleChalled(SocketChannel channel, ByteBuffer buf) throws IOException;
    }

    private static class ChannelReadHandler extends ChannelHandler {

	@Override
	protected int getInterest() {
	    return SelectionKey.OP_READ;
	}

	@Override
	protected void handleChalled(SocketChannel channel, ByteBuffer buf) throws IOException {
	    if (channel.read(buf) < 0) {
		throw new EOFException("EOF on input socket (fillBuffer)");
	    }
	}
    }

    private static class ChannelWriteHandler extends ChannelHandler {

	@Override
	protected int getInterest() {
	    return SelectionKey.OP_WRITE;
	}

	@Override
	protected void handleChalled(SocketChannel channel, ByteBuffer buf) throws IOException {
	    channel.write(buf);
	}
    }

    private static class DcapCommandBuilder {

        private final ByteBuffer _command;

        DcapCommandBuilder() {
            _command = ByteBuffer.allocate(8192);
            _command.order(ByteOrder.BIG_ENDIAN);
            _command.position(Integer.SIZE / 8);
        }

        DcapCommandBuilder withSeekAndRead(long offset, long len) {
            _command.putInt(SEEK_AND_READ);
            _command.putLong(offset);
            _command.putInt(SEEK_SET);
            _command.putLong(len);
            return this;
        }

        DcapCommandBuilder withSeekAndWrite(long offset, int len) {
            _command.putInt(SEEK_AND_WRITE);
            _command.putLong(offset);
            _command.putInt(SEEK_SET);
            return this;
        }

        DcapCommandBuilder withClose() {
            _command.putInt(CLOSE);
            return this;
        }

        DcapCommandBuilder withByteCount(long count) {
            _command.putLong(count);
            return this;
        }

        ByteBuffer build() {
            _command.putInt(0, _command.position() - 4);
            _command.flip();
            return _command;
        }
    }
}
