package publisher;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class PublisherMain {
    private static final int PUBLISHER_PORT = 12349;

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(PUBLISHER_PORT)) {
            System.out.println("\n====================================================");
            System.out.println("[MARS TRANSACTION REPORTING PUBLISHER ACTIVE]");
            System.out.println("Listening for Transition Engine metrics on port: " + PUBLISHER_PORT);
            System.out.println("====================================================\n");

            while (!serverSocket.isClosed()) {
                try {
                    Socket socket = serverSocket.accept();
                    new Thread(() -> handleIncomingReport(socket)).start();
                } catch (IOException e) {
                    System.err.println("[PUBLISHER ERROR] Connection intercept failed: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("[PUBLISHER CRITICAL] Server socket crash: " + e.getMessage());
        }
    }
    private static void handleIncomingReport(Socket socket) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
            String rawReport = reader.readLine();
            if (rawReport == null || rawReport.trim().isEmpty()) return;

            String[] systemTiers = rawReport.split("\\|\\|");
            if (systemTiers.length < 3) return;

            String[] engineSourceInfo = systemTiers[0].split("\\|");
            String[] relationalInfo = systemTiers[1].split(";");
            String rawMetadataFields = systemTiers[2].replace("[", "").replace("]", "");

            String containerHost = engineSourceInfo[0];
            String containerPort = engineSourceInfo[1];
            String eventID = relationalInfo[0];
            String submissionCount = relationalInfo[1];

            String trackID = "UNKNOWN", source = "UNKNOWN", traceID = "UNKNOWN", msgContent = "UNKNOWN";
            String[] metadataTokens = rawMetadataFields.split(" \\| ");
            for (String token : metadataTokens) {
                String[] kv = token.split(": ");
                if (kv.length == 2) {
                    if (kv[0].trim().equals("TRACK ID")) trackID = kv[1].trim();
                    if (kv[0].trim().equals("SOURCE")) source = kv[1].trim();
                    if (kv[0].trim().equals("TRACE ID")) traceID = kv[1].trim();
                    if (kv[0].trim().equals("MESSAGE")) msgContent = kv[1].trim();
                }
            }

            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));

            System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════════════╗");
            System.out.println("║                🚨 REAL-TIME MARS PIPELINE TRANSACTION EVENT SUMMARY 🚨               ║");
            System.out.println("╠══════════════════════════════════════════════════════════════════════════════════════╣");
            System.out.printf("  [PUBLISHED AT]     %s\n", timestamp);
            System.out.printf("  [EVENT LOG IDENTITY]  %s (Unified Relational State Envelope Link)\n", eventID);
            System.out.printf("  [RUNNING GROUP COUNT] This correlated container track has processed %s total rows\n", submissionCount);
            System.out.println("╟──────────────────────────────────────────────────────────────────────────────────────╢");
            System.out.println("  💡 INGESTED MESSAGE STRUCTURAL DATA RECOVERY:");
            System.out.printf("    • Correlated TrackID : %s\n", trackID);
            System.out.printf("    • Origin Source User : %s\n", source);
            System.out.printf("    • Unique TraceID ID  : %s\n", traceID);
            System.out.printf("    • Payload Content    : %s\n", msgContent);
            System.out.println("╟──────────────────────────────────────────────────────────────────────────────────────╢");
            System.out.println("  🛡️ PROCESSING CONTAINER METRICS LOCATION SPECIFICS:");
            System.out.printf("    • Active Docker Node Name : %s\n", containerHost);
            System.out.printf("    • Boundary Listening Port : %s\n", containerPort);
            System.out.println("╚══════════════════════════════════════════════════════════════════════════════════════╝\n");

        } catch (Exception e) {
            System.err.println("[PUBLISHER PROCESSING ERROR] Summary generation exception: " + e.getMessage());
        } finally {
            try { socket.close(); } catch (Exception ignored) {}
        }
    }
}
