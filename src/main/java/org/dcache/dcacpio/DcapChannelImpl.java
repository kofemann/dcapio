package org.dcache.dcacpio;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class DcapChannelImpl implements DcapChannel {

    private SocketChannel _channel;

    public DcapChannelImpl(String host, int port, byte[] chalange) throws IOException {
        _channel = SocketChannel.open(new InetSocketAddress(host, port));
        ByteBuffer buf = ByteBuffer.allocate(8+chalange.length);
        buf.putInt(chalange.length).put(chalange);
        buf.flip();
        _channel.write(buf);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return 0;
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        return 0;
    }
}
