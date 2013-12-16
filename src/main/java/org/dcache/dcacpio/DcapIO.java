package org.dcache.dcacpio;

import com.google.common.base.Charsets;
import com.google.common.net.HostAndPort;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class DcapIO {

    private final String HELLO = "%d 0 client hello 0 0 0 0";
    private final String BYE   = "%d 0 client byebye";
    private final String OPEN  = "%d 0 client open dcap://%s/%s %s localhost 1111 -passive";

    private final InetSocketAddress _door;
    private int _sequence = 0;
    private SocketChannel _channel;
    private final String _uri;
    private final ByteBuffer _messageBuffer = ByteBuffer.allocate(8192);

    public DcapIO(String uri) {
        _uri = uri;
        HostAndPort hostAndPort = HostAndPort.fromString(_uri);
        _door = new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPort());
    }

    public synchronized void connect() throws IOException {
        _channel = SocketChannel.open(_door);
        sayHello();
    }

    public synchronized void disconnect() throws IOException {
        sayByeBye();
        _channel.close();
    }

    public synchronized DcapChannel open(String path, String mode) throws IOException {
        int session = nextSequence();
        String open = String.format(OPEN, session, _uri, path, mode);
        sendControlMessage(open);
        String message = getControlMessage();

        String[] replys = message.split(" ");
        String host = replys[4];
        int port = Integer.parseInt(replys[5]);
        byte[] challange = replys[6].getBytes(Charsets.US_ASCII);

        return new DcapChannelImpl(host, port, session, challange);
    }

    private void sayHello() throws IOException {
        String hello = String.format(HELLO, nextSequence());
        sendControlMessage(hello);
        getControlMessage();
    }

    private void sayByeBye() throws IOException {
        String bye = String.format(BYE, nextSequence());
        sendControlMessage(bye);
        getControlMessage();
    }

    private int nextSequence() {
        return _sequence++;
    }

    private void sendControlMessage(String message) throws IOException {
        _messageBuffer.clear();
        _messageBuffer.put(message.getBytes(Charsets.US_ASCII));
        _messageBuffer.put("\r\n".getBytes(Charsets.US_ASCII));
        _messageBuffer.flip();
        _channel.write(_messageBuffer);
    }

    private String getControlMessage() throws IOException {
        boolean done = false;
        StringBuilder sb = new StringBuilder();
        while (!done) {
            _messageBuffer.clear();
            _channel.read(_messageBuffer);
            String message = new String(_messageBuffer.array(),
                    0, _messageBuffer.position(), Charsets.US_ASCII);
            sb.append(message);
            if (sb.charAt(sb.length() - 1) == '\n') {
                done = true;
            }
        }

        // remove trailing \n\r

        while(sb.charAt(sb.length() - 1) == '\n' || sb.charAt(sb.length() - 1) == '\r') {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        DcapIO dcap = new DcapIO("dcache-lab000:22125");
        dcap.connect();
        try(DcapChannel dcapChannel = dcap.open("/exports/data/p34", "r")) {
            ByteBuffer in = ByteBuffer.allocate(8192);

            // get full data
            while(dcapChannel.read(in, in.position()) > 0) ;
            in.flip();
            System.out.println( new String(in.array(), 0, in.remaining(), Charsets.US_ASCII));
        }
        dcap.disconnect();        
    }

}
