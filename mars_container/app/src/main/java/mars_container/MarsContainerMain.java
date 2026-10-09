package mars_container;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class MarsContainerMain {
    private static final String DB_URL  = "jdbc:oracle:thin:@oracle-enterprise-db:1521/ORCLCDB";
    private static final String DB_USER = "system";
    private static final String DB_PASS = "password123";

    private static final ExecutorService pipelineExecutor = Executors.newFixedThreadPool(10);

    public static void main(String[] args) {
        System.setProperty("oracle.net.disableNio", "true");

        int tempPort = 12346; 
        if (args != null && args.length > 0) {
            // THE BRACKETED ARRAY FIX: Safely accesses index 0 to parse port assignment variables
            try { tempPort = Integer.parseInt(args[0].trim()); } catch (Exception ignored) {}
        }
        final int port = tempPort;

        try {
            Class.forName("oracle.jdbc.OracleDriver");
            System.out.println("\n====================================================");
            System.out.println("[MARS CONTAINER ACTIVE - ON-DEMAND ARRAY MATCHING]");
            System.out.println("Java-Side Stateless Processor listening on port: " + port);
            System.out.println("Database endpoint mapping: " + DB_URL);
            System.out.println("====================================================\n");

            ServerSocket serverSocket = new ServerSocket(port);
            Thread acceptThread = new Thread(() -> {
                while (!serverSocket.isClosed()) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        pipelineExecutor.submit(() -> readClientDataStream(clientSocket, port));
                    } catch (IOException e) {
                        System.err.println("[NETWORK ERROR] Socket accept failed: " + e.getMessage());
                    }
                }
            });
            acceptThread.setName("Mars-Network-Listener");
            acceptThread.start();
            acceptThread.join();
        } catch (Exception e) { 
            System.err.println("\n[MARS CONTAINER FATAL EXCEPTION] Runtime trap: " + e.getMessage() + "\n"); 
        }
    }

    private static void readClientDataStream(Socket socket, int port) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true)) {
            
            String rawJson;
            while ((rawJson = reader.readLine()) != null) {
                rawJson = rawJson.trim();
                if (!rawJson.isEmpty()) {
                    String summaryAck = processMessageAndSyncDatabase(rawJson, port);
                    writer.println(summaryAck);
                }
            }
        } catch (IOException e) {
            System.err.println("[NETWORK ERROR] Connection read/write interrupted: " + e.getMessage());
        } finally {
            try { socket.close(); } catch (Exception ignored) {}
        }
    }
    private static String processMessageAndSyncDatabase(String rawJson, int activePort) {
        Map<String, String> parsed = parseJsonRobust(rawJson);
        String trackID = parsed.get("trackID");
        String source = parsed.get("source");
        String traceID = parsed.get("traceID");
        String message = parsed.get("message");

        if (trackID == null || source == null || traceID == null) {
            System.err.println("[MARS CONTAINER DROP WARNING] Missing key routing parameters!");
            return "ERROR:Missing mandatory parameters||";
        }

        System.out.println("\n[MARS CONTAINER ON PORT " + activePort + "] Targeted matching started for TrackID: " + trackID);

        Connection conn = null;
        PreparedStatement checkStmt = null;
        PreparedStatement parentStmt = null;
        PreparedStatement fetchHistoryStmt = null;
        PreparedStatement childStmt = null;
        PreparedStatement countStmt = null;
        ResultSet rs = null;

        long targetEventId = -1;
        int totalSubmissions = 0;
        
        // Changing history list populated on-demand for algorithmic processing
        List<String> activeTrackingEventList = new ArrayList<>();

        try {
            Properties props = new Properties();
            props.put("user", DB_USER);
            props.put("password", DB_PASS);
            props.put("oracle.net.disableNio", "true"); 

            conn = DriverManager.getConnection(DB_URL, props);
            conn.setAutoCommit(false); 

            // 1. PINPOINTED QUERY: Check if a parent event envelope already exists for this track
            String checkSql = "SELECT event_id FROM app_events WHERE track_id = ?";
            checkStmt = conn.prepareStatement(checkSql);
            checkStmt.setString(1, trackID);
            rs = checkStmt.executeQuery();

            if (rs.next()) {
                targetEventId = rs.getLong("event_id");
                System.out.println("[JDBC MATCH FOUND] Associated with Event ID: " + targetEventId);
            }
            rs.close();
            checkStmt.close();

            // 2. CREATE EVENT: Provision a new parent envelope if this is a brand-new track
            if (targetEventId == -1) {
                String insertParentSql = "INSERT INTO app_events(track_id) VALUES(?)";
                parentStmt = conn.prepareStatement(insertParentSql, new String[]{"event_id"});
                parentStmt.setString(1, trackID);
                parentStmt.executeUpdate();

                ResultSet generatedKeys = parentStmt.getGeneratedKeys();
                if (generatedKeys.next()) {
                    targetEventId = generatedKeys.getLong(1);
                    System.out.println("[JDBC INSERT MASTER] Created Parent Event ID: " + targetEventId);
                }
                generatedKeys.close();
                parentStmt.close();
            } else {
                // 3. ON-DEMAND ARRAY POPULATION: Pull history strictly for this track_id from database
                String historySql = "SELECT message_content FROM event_messages WHERE event_id = ? ORDER BY logged_at ASC";
                fetchHistoryStmt = conn.prepareStatement(historySql);
                fetchHistoryStmt.setLong(1, targetEventId);
                rs = fetchHistoryStmt.executeQuery();
                while (rs.next()) {
                    activeTrackingEventList.add(rs.getString("message_content"));
                }
                rs.close();
                fetchHistoryStmt.close();
            }

            // 4. APPEND THE NEW ITEM: Combine historical data rows with the newest arriving packet row
            activeTrackingEventList.add(message);

            // =========================================================================
            // INJECT ADVANCED ALGORITHM LOGIC HERE
            // =========================================================================
            System.out.println("[ALGORITHM LAYER] Processing active event list array (Size: " + activeTrackingEventList.size() + ") for Track: " + trackID);

            // 5. UPDATE WRITEBACK: Insert the fresh transaction row under the event link
            String insertChildSql = "INSERT INTO event_messages(event_id, source, trace_id, message_content) VALUES(?, ?, ?, ?)";
            childStmt = conn.prepareStatement(insertChildSql);
            childStmt.setLong(1, targetEventId);
            childStmt.setString(2, source);
            childStmt.setString(3, traceID);
            childStmt.setString(4, message);
            childStmt.executeUpdate();
            childStmt.close();

            // 6. METRICS COUNT: Grab updated count for the publisher dashboard return trip
            String countSql = "SELECT COUNT(*) FROM event_messages WHERE event_id = ?";
            countStmt = conn.prepareStatement(countSql);
            countStmt.setLong(1, targetEventId);
            ResultSet countRs = countStmt.executeQuery();
            if (countRs.next()) {
                totalSubmissions = countRs.getInt(1);
            }
            countRs.close();
            countStmt.close();

            conn.commit();
            System.out.println("[JDBC TRANSACTION] Commit successful. Single-step read, process, and write finalized.\n");

        } catch (Exception ex) {
            System.err.println("\n[MARS CONTAINER DATABASE EXCEPTION] Fail during transaction execution!");
            System.err.println("Error Message: " + ex.getMessage());
            if (conn != null) {
                try { conn.rollback(); } catch (SQLException e) { System.err.println("Rollback failed: " + e.getMessage()); }
            }
            return "ERROR:Database failure||";
        } finally {
            if (conn != null) { try { conn.close(); } catch (Exception ignored) {} }
        }

        return "EVT-" + targetEventId + ";" + totalSubmissions + "||[TRACK ID: " + trackID + " | SOURCE: " + source + " | TRACE ID: " + traceID + " | ARRAY SIZE: " + activeTrackingEventList.size() + "]";
    }

    private static Map<String, String> parseJsonRobust(String json) {
        Map<String, String> map = new HashMap<>();
        String clean = json.trim();
        if (clean.startsWith("{")) clean = clean.substring(1);
        if (clean.endsWith("}")) clean = clean.substring(0, clean.length() - 1);
        
        String[] tokens = clean.split(",");
        for (String token : tokens) {
            int colonIdx = token.indexOf(":");
            if (colonIdx != -1) {
                String key = token.substring(0, colonIdx).replace("\"", "").trim();
                String val = token.substring(colonIdx + 1).replace("\"", "").trim();
                map.put(key, val);
            }
        }
        return map;
    }
}
