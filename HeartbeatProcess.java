import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.util.HashMap;
import java.util.Map;

public class HeartbeatProcess {

    private static final String MULTICAST_GROUP = "230.0.0.0";
    private static final int PORT = 4446;
    private static final int HEARTBEAT_INTERVAL = 2000; // 2 secunde
    private static final int TIMEOUT_INTERVAL = 5000;   // 5 secunde
    private String processId;
    private Map<String, Long> lastHeartbeat = new HashMap<>();

    public HeartbeatProcess(String processId) {
        this.processId = processId;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Usage: java HeartbeatProcess <processId>");
            return;
        }

        HeartbeatProcess process = new HeartbeatProcess(args[0]);
        process.start();
    }

    public void start() throws IOException {
        InetAddress group = InetAddress.getByName(MULTICAST_GROUP);
        NetworkInterface networkInterface = NetworkInterface.getByInetAddress(InetAddress.getLocalHost());
        MulticastSocket socket = new MulticastSocket(PORT);
        
        // Join multicast group using the new method with NetworkInterface
        socket.joinGroup(new InetSocketAddress(group, PORT), networkInterface);

        // Thread pentru trimiterea heartbeat-urilor
        new Thread(() -> {
            while (true) {
                try {
                    String heartbeatMessage = "HEARTBEAT:" + processId;
                    byte[] buffer = heartbeatMessage.getBytes();
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, PORT);
                    socket.send(packet);
                    Thread.sleep(HEARTBEAT_INTERVAL);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        // Thread pentru ascultarea mesajelor
        new Thread(() -> {
            while (true) {
                try {
                    byte[] buffer = new byte[256];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);
                    String message = new String(packet.getData(), 0, packet.getLength());

                    if (message.startsWith("HEARTBEAT:")) {
                        String otherProcessId = message.split(":")[1];
                        lastHeartbeat.put(otherProcessId, System.currentTimeMillis());
                    } else if (message.startsWith("MESSAGE:")) {
                        System.out.println("Received message: " + message);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        // Thread pentru verificarea defectării
        new Thread(() -> {
            while (true) {
                long currentTime = System.currentTimeMillis();
                for (Map.Entry<String, Long> entry : lastHeartbeat.entrySet()) {
                    if (currentTime - entry.getValue() > TIMEOUT_INTERVAL) {
                        System.out.println("Process " + entry.getKey() + " has failed!");
                    }
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        // Citire de la consola pentru trimiterea mesajelor
        while (true) {
    byte[] buffer = new byte[256];
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, PORT);
    String inputMessage = System.console().readLine();
    if (inputMessage.equalsIgnoreCase("exit")) {
        break;
    }
    String fullMessage = "MESSAGE:" + processId + ": " + inputMessage;
    buffer = fullMessage.getBytes();
    packet = new DatagramPacket(buffer, buffer.length, group, PORT);
    socket.send(packet);
}

        // Leave multicast group using the new method with NetworkInterface
        socket.leaveGroup(new InetSocketAddress(group, PORT), networkInterface);
        socket.close();
    }
}
