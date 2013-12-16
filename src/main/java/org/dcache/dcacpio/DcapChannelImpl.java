package org.dcache.dcacpio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

public class DcapChannelImpl implements DcapChannel {

    /**
     * DCAP binary commands
     */
    private static final int CLOSE = 4;
    private static final int SEEK_AND_READ = 11;
    private static final int SEEK_AND_WRITE = 12;

    /**
     * DCAP SEEK whence
     */
    private static final int SEEK_SET = 0;

    private final SocketChannel _channel;

    public DcapChannelImpl(String host, int port, int session, byte[] challange) throws IOException {
        _channel = SocketChannel.open(new InetSocketAddress(host, port));
        _channel.configureBlocking(true);
        ByteBuffer buf = ByteBuffer.allocate(8 + challange.length);
        buf.order(ByteOrder.BIG_ENDIAN);
        buf.putInt(session);
        buf.putInt(challange.length).put(challange);
        buf.flip();
        writeFully(_channel, buf);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {

        ByteBuffer command = new DcapCommandBuilder()
                .withSeekAndRead(position, dst.remaining())
                .build();

        writeFully(_channel, command);
        getAck();
        return getData(dst);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        ByteBuffer command = new DcapCommandBuilder()
                .withClose()
                .build();

        writeFully(_channel, command);
        getAck();
        _channel.close();
    }

    private int getData(ByteBuffer buf) throws IOException {
        ByteBuffer dataBlock = ByteBuffer.allocate(128);
        dataBlock.order(ByteOrder.BIG_ENDIAN);

        dataBlock.limit(8);
        _channel.read(dataBlock);
        int total = 0;
        while (true) {
            dataBlock.clear();
            dataBlock.limit(4);
            _channel.read(dataBlock);
            dataBlock.flip();
            int n = dataBlock.getInt();
            if (n < 0) {
                break;
            }

            ByteBuffer chunk = buf.slice();
            chunk.limit(n);
            _channel.read(chunk);
            buf.position(buf.position() + n);
            total += n;
        }
        getAck();
        return total;
    }

    private void getAck() throws IOException {
        ByteBuffer ackBuffer = ByteBuffer.allocate(256);
        ackBuffer.order(ByteOrder.BIG_ENDIAN);
        ackBuffer.limit(4);
        _channel.read(ackBuffer);
        ackBuffer.flip();
        int len = ackBuffer.getInt();
        ackBuffer.clear().limit(len);
        _channel.read(ackBuffer);
        // FIXME: error handling
    }

    private static void writeFully(SocketChannel channel, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
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
