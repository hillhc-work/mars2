package transition_engine;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TransitionEngineMain {
    private static final int SERVER_PORT = 12345;
    private static final String STATE_FILE = "routing_state_map.db";
    
    private static final String[] CONTAINER_HOSTS = {"mars-server-1", "mars-server-2", "mars-server-3"};
    private static final int[] CONTAINER_PORTS = {12346, 12347, 12348}; 
    private static final int TOTAL_CONTAINERS = CONTAINER_PORTS.length;

    private static final ConcurrentHashMap<String, Integer> affinityCache = new ConcurrentHashMap<>();
    private static final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    public static void main(String[] args) {
        System.out.println("\n====================================================================");
        System.out.println("[TRANSITION ENGINE INITIALIZATION] Initializing system clusters...");
        System.out.println("Configured active balance topologies count: " + TOTAL_CONTAINERS);
        System.out.println("====================================================\n");
        
        loadRoutingCacheState();
        
        try {
            final AsynchronousServerSocketChannel server = AsynchronousServerSocketChannel.open()
                    .bind(new InetSocketAddress("0.0.0.0", SERVER_PORT));
            System.out.println("\n[TRANSITION ENGINE] Middle server listening smoothly on port: " + SERVER_PORT + "\n");

            server.accept(null, new CompletionHandler<AsynchronousSocketChannel, Void>() {
                @Override
                public void completed(AsynchronousSocketChannel client, Void attach) {
                    server.accept(null, this);
                    System.out.println("\n[TRANSITION ENGINE CONNECTION] Network stream attached from Driver.\n");
                    ByteBuffer buf = ByteBuffer.allocate(16384);
                    StringBuilder acc = new StringBuilder();
                    readDriverTextStream(client, buf, acc);
                }
                @Override public void failed(Throwable e, Void a) {}
            });
            Thread.currentThread().join();
        } catch (Exception e) { 
            System.err.println("\n[TRANSITION ENGINE ERROR] Fatal system runtime trap: " + e.getMessage() + "\n"); 
        }
    }

    private static synchronized void loadRoutingCacheState() {
        try {
            Path path = Paths.get(STATE_FILE);
            if (!Files.exists(path)) return;
            List<String> lines = Files.readAllLines(path);
            for (String line : lines) {
                String[] tokens = line.split("=");
                if (tokens.length == 2) {
                    affinityCache.put(tokens[0].trim(), Integer.parseInt(tokens[1].trim()));
                }
            }
        } catch (Exception ignored) {}
    }
    private static synchronized void persistRouteToCache(String key, int containerIndex) {
        try {
            affinityCache.put(key, containerIndex);
            try (FileWriter fw = new FileWriter(STATE_FILE, true); PrintWriter pw = new PrintWriter(fw)) {
                pw.println(key + "=" + containerIndex);
            }
        } catch (Exception ignored) {}
    }

    private static void readDriverTextStream(final AsynchronousSocketChannel client, final ByteBuffer buf, final StringBuilder acc) {
        client.read(buf, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void att) {
                if (result <= 0) { 
                    try { client.close(); } catch (Exception ignored) {}
                    return; 
                }
                buf.flip();
                byte[] bytes = new byte[buf.remaining()];
                buf.get(bytes);
                buf.clear();
                acc.append(new String(bytes, StandardCharsets.UTF_8));

                int newlineIndex;
                while ((newlineIndex = acc.indexOf("\n")) != -1) {
                    String rawLine = acc.substring(0, newlineIndex).trim();
                    acc.delete(0, newlineIndex + 1);
                    if (rawLine.isEmpty()) continue;

                    System.out.println("\n[TRANSITION ENGINE RECEIVED] Inbound string line line: " + rawLine);

                    Map<String, String> parsed = extractFields(rawLine);
                    String trackID = parsed.getOrDefault("trackID", "UNKNOWN");
                    String source = parsed.getOrDefault("source", "UNKNOWN");
                    
                    String lookupKey = trackID + "::" + source;
                    int targetIndex;
                    boolean cacheHit = false;

                    if (affinityCache.containsKey(lookupKey)) {
                        targetIndex = affinityCache.get(lookupKey);
                        cacheHit = true;
                    } else {
                        targetIndex = Math.abs(roundRobinCounter.getAndIncrement()) % TOTAL_CONTAINERS;
                        persistRouteToCache(lookupKey, targetIndex);
                    }

                    String targetHost = CONTAINER_HOSTS[targetIndex];
                    int targetPort = CONTAINER_PORTS[targetIndex];

                    System.out.println("[TRANSITION ENGINE ROUTING] Routing combo [" + lookupKey + "] (Cache-Hit: " + cacheHit + ") -> Targeted container: " + targetHost + " on port " + targetPort);

                    String cleanJson = convertMapToJson(parsed) + "\n";
                    forwardToContainer(cleanJson, targetHost, targetPort);
                }
                client.read(buf, null, this);
            }
            @Override public void failed(Throwable e, Void a) { try { client.close(); } catch (Exception ignored) {} }
        });
    }

    private static Map<String, String> extractFields(String rawLine) {
        Map<String, String> map = new HashMap<>();
        String[] pairs = rawLine.split(";");
        for (String pair : pairs) {
            String[] parts = pair.split(":");
            if (parts.length >= 3) {
                map.put(parts[0].trim(), parts[2].trim());
            }
        }
        return map;
    }

    private static String convertMapToJson(Map<String, String> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(entry.getKey()).append("\":\"").append(entry.getValue().replace("\"", "\\\"")).append("\"");
            first = false;
        }
        return sb.append("}").toString();
    }

    private static void forwardToContainer(String payload, String host, int port) {
        try {
            final AsynchronousSocketChannel containerClient = AsynchronousSocketChannel.open();
            containerClient.connect(new InetSocketAddress(host, port), null, new CompletionHandler<Void, Void>() {
                @Override
                public void completed(Void res, Void att) {
                    ByteBuffer sendBuf = ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8));
                    containerClient.write(sendBuf, null, new CompletionHandler<Integer, Void>() {
                        @Override
                        public void completed(Integer w, Void a) {
                            final ByteBuffer ackBuf = ByteBuffer.allocate(8192);
                            containerClient.read(ackBuf, null, new CompletionHandler<Integer, Void>() {
                                @Override
                                public void completed(Integer r, Void av) {
                                    if (r > 0) {
                                        ackBuf.flip();
                                        byte[] b = new byte[ackBuf.remaining()];
                                        ackBuf.get(b);
                                        String ack = new String(b, StandardCharsets.UTF_8).trim();
                                        
                                        String consolidatedReport = host + "|" + port + "||" + ack + "\n";
                                        sendToPublisher(consolidatedReport);
                                    }
                                    try { containerClient.close(); } catch (Exception ignored) {}
                                }
                                @Override public void failed(Throwable e, Void av) { try { containerClient.close(); } catch (Exception ignored) {} }
                            });
                        }
                        @Override public void failed(Throwable e, Void a) { try { containerClient.close(); } catch (Exception ignored) {} }
                    });
                }
                @Override public void failed(Throwable e, Void a) { try { containerClient.close(); } catch (Exception ignored) {} }
            });
        } catch (IOException ignored) {}
    }

    private static void sendToPublisher(String summary) {
        try {
            final AsynchronousSocketChannel pubClient = AsynchronousSocketChannel.open();
            pubClient.connect(new InetSocketAddress("reporting-publisher-server", 12349), null, new CompletionHandler<Void, Void>() {
                @Override
                public void completed(Void res, Void att) {
                    ByteBuffer sendBuf = ByteBuffer.wrap(summary.getBytes(StandardCharsets.UTF_8));
                    pubClient.write(sendBuf, null, new CompletionHandler<Integer, Void>() {
                        @Override public void completed(Integer w, Void a) { try { pubClient.close(); } catch (Exception ignored) {} }
                        @Override public void failed(Throwable e, Void a) { try { pubClient.close(); } catch (Exception ignored) {} }
                    });
                }
                @Override public void failed(Throwable e, Void a) { try { pubClient.close(); } catch (Exception ignored) {} }
            });
        } catch (IOException ignored) {}
    }
}
