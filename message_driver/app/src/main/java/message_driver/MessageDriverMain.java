package message_driver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.*;

public class MessageDriverMain {
    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        Path userDir = Paths.get(".").toAbsolutePath().normalize();
        Path inputDirCheck = Paths.get("data_input_files").toAbsolutePath().normalize();
        
        System.out.println("\n====================================================");
        System.out.println("[MESSAGE DRIVER ENVIRONMENT DIAGNOSTICS]");
        System.out.println("Current Working Directory (user.dir) : " + userDir);
        System.out.println("Target Input Path Java evaluates    : " + inputDirCheck);
        System.out.println("Does Target Input Path exist?        : " + Files.exists(inputDirCheck));
        
        if (Files.exists(inputDirCheck)) {
            try (Stream<Path> s = Files.walk(inputDirCheck)) {
                long count = s.filter(Files::isRegularFile).count();
                System.out.println("Total usable text files discovered   : " + count);
            }
        }
        System.out.println("====================================================\n");

        boolean fileMode = false;
        for (String arg : args) {
            if ("--file-mode".equalsIgnoreCase(arg) || "-f".equalsIgnoreCase(arg)) {
                fileMode = true;
                break;
            }
        }
        if (fileMode) startFileMode(); else startSocketMode();
        shutdownLatch.await();
    }

    private static void startSocketMode() throws IOException {
        Path inputDir = Paths.get("data_input_files").toAbsolutePath().normalize();
        if (!Files.exists(inputDir)) { 
            System.err.println("[DIAGNOSTIC ERROR] data_input_files folder not found relative to runtime!");
            shutdownLatch.countDown(); 
            return; 
        }
        
        final List<Path> messageFiles;
        try (Stream<Path> walk = Files.walk(inputDir)) {
            messageFiles = walk.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".txt"))
                    .sorted().collect(Collectors.toList());
        }
        if (messageFiles.isEmpty()) { 
            System.err.println("[DIAGNOSTIC ERROR] Target folder exists but contains 0 matching .txt files!");
            shutdownLatch.countDown(); 
            return; 
        }

        final AsynchronousSocketChannel client = AsynchronousSocketChannel.open();
        
        // CORRECTION: Target the explicit container network DNS name instead of 127.0.0.1
        System.out.println("[MESSAGE DRIVER] Connecting to bridge network endpoint: transition-engine-server:12345");
        client.connect(new InetSocketAddress("transition-engine-server", 12345), null, new CompletionHandler<Void, Void>() {
            private final AtomicInteger fileIndex = new AtomicInteger(0);
            private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            @Override
            public void completed(Void result, Void attachment) {
                System.out.println("[MESSAGE DRIVER] Connected to Transition Engine! Starting stream...");
                scheduler.scheduleAtFixedRate(() -> {
                    int idx = fileIndex.getAndIncrement();
                    if (idx >= messageFiles.size()) {
                        scheduler.shutdown();
                        try { client.close(); } catch (Exception ignored) {}
                        shutdownLatch.countDown();
                        return;
                    }
                    try {
                        Path targetFile = messageFiles.get(idx);
                        String contents = Files.readString(targetFile, StandardCharsets.UTF_8).trim();
                        Random random = new Random();

                        if (!contents.contains("traceID")) {
                            String patchedTrace = "traceID:" + random.nextInt(10) + ":TRC-PATCHED-" + UUID.randomUUID().toString().substring(0, 5);
                            contents = patchedTrace + ";" + contents;
                        }
                        
                        if (contents.contains("timestamp")) {
                            contents = contents.replaceAll("timestamp:[0-9]:[0-9]+", "timestamp:" + random.nextInt(10) + ":" + System.currentTimeMillis());
                        } else {
                            contents += ";timestamp:" + random.nextInt(10) + ":" + System.currentTimeMillis();
                        }

                        String networkPayload = contents + "\n";
                        System.out.println("[MESSAGE DRIVER SENT] Transmitting row: " + targetFile.getFileName());

                        client.write(ByteBuffer.wrap(networkPayload.getBytes(StandardCharsets.UTF_8)), null, new CompletionHandler<Integer, Void>() {
                            @Override public void completed(Integer r, Void a) {}
                            @Override public void failed(Throwable e, Void a) {}
                        });
                    } catch (IOException ignored) {}
                }, 0, 1, TimeUnit.SECONDS);
            }
            @Override public void failed(Throwable exc, Void attachment) { 
                System.err.println("[MESSAGE DRIVER ERROR] Transition Engine connection failed.");
                shutdownLatch.countDown(); 
            }
        });
    }

    private static void startFileMode() throws IOException {
        Path outputDir = Paths.get("data_input_files").toAbsolutePath().normalize();
        if (!Files.exists(outputDir)) Files.createDirectories(outputDir);

        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        final AtomicInteger msgCount = new AtomicInteger(0);

        scheduler.scheduleAtFixedRate(() -> {
            int id = msgCount.incrementAndGet();
            if (id > 300) { scheduler.shutdown(); shutdownLatch.countDown(); return; }

            Random random = new Random();
            String traceID = "TRC-" + UUID.randomUUID().toString().substring(0, 8);
            String trackID = "TRACK-" + (100 + random.nextInt(1000));
            String source = "SRC-" + random.nextInt(10);
            String message = "Processing variant request item number " + id;

            List<String> textPairs = new ArrayList<>();
            textPairs.add("traceID:" + random.nextInt(10) + ":" + traceID);
            textPairs.add("trackID:" + random.nextInt(10) + ":" + trackID);
            textPairs.add("source:" + random.nextInt(10) + ":" + source);
            textPairs.add("message:" + random.nextInt(10) + ":" + message);

            if (random.nextBoolean()) {
                textPairs.add("recordID:" + random.nextInt(10) + ":REC-" + random.nextInt(100));
            }

            try {
                Files.writeString(outputDir.resolve(String.format("msg_%03d.txt", id)), String.join(";", textPairs), StandardCharsets.UTF_8);
            } catch (IOException ignored) {}
        }, 0, 100, TimeUnit.MILLISECONDS);
    }
}
