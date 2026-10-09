#this is the docker compose file that would allow a second transition engine to have access to the same resources

services:

  # Primary Routing Engine Engine
  transition-engine-primary:
    container_name: transition-engine-server
    build:
      context: ./transition_engine/app
      dockerfile: Dockerfile
    ports:
      - "12345:12345"
    volumes:
      # Mounts the shared database folder inside the primary container
      - cache-state-shared:/app
    environment:
      - JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF_8
    networks:
      - mars-pipeline-net
    depends_on:
      - mars-server-1
      - mars-server-2
      - mars-server-3

  # STANDBY BACKUP ROUTING ENGINE
  transition-engine-backup:
    container_name: transition-engine-backup
    build:
      context: ./transition_engine/app
      dockerfile: Dockerfile
    ports:
      - "12344:12345" # Listens on a separate host port (12344) but talks to the same cluster
    volumes:
      # Mounts the EXACT SAME shared storage space inside the backup container
      - cache-state-shared:/app
    environment:
      - JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF_8
    networks:
      - mars-pipeline-net
    depends_on:
      - mars-server-1
      - mars-server-2
      - mars-server-3

# Declare the shared named volume space at the root level of your file
volumes:
  cache-state-shared:

