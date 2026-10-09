-------------------------------------------------------------------------------------------------
				General build instructions
-------------------------------------------------------------------------------------------------
- start the publisher before the transition engine, the transition engine connects to it
- start the mars containers first, the transition engine connects to them
- start the transition engine second, the message driver uses it as its server
- start the message driver last, it initiates the message passing
- you need to have a database, the schema is present if not and instructions for prototype usage

-------------------------------------------------------------------------------------------------
		    Building them all and then using docker compose
-------------------------------------------------------------------------------------------------
# Compile all jars cleanly at once
cd ~/Workspace/java_projects/mars_mvp

# 1. Compile the fresh .jar binaries on your host machine
(cd publisher/app && ../gradlew clean build -x test)
(cd mars_container/app && ../gradlew clean build -x test)
(cd transition_engine/app && ../gradlew clean build -x test)
(cd message_driver/app && ../gradlew clean build -x test)

# 2. Tell Compose to wipe the old images and re-package the new jars 
#(--build rebuilds everything, up just starts without --build)
sudo docker compose up --build

------------------------------------------------------------------

#end to end with all servers staying up
# Step A: Spin up all 6 servers silently in the background
sudo docker compose up -d
# Step B: Attach a continuous, live log monitor tail to watch the data flow
sudo docker compose logs -f
# Step C: (Run from a 2nd terminal tab whenever you want to re-trigger a fresh 300-message run)
sudo docker compose start message-driver-stream

#shutdown docker
sudo docker compose down

-------------------------------------------------------------------------------------------------
		Running and building the message driver (message_driver gradle project)
-------------------------------------------------------------------------------------------------
#The message driver creates 300 files when run in file mode, change this to the desired limit
#make sure to follow the naming structure if your files already exist.

#The Message Driver reads files strictly based on their extension, alphabetical order, and content structure, 
#meaning it does not care whether a file was built by an automated script or typed by hand.

#Missing traceID? The driver will see it is missing, auto-generate a fresh traceID:RANDOM_DIGIT:TRC-PATCHED-XXXXX, 
#and append it to the front of your string.Missing timestamp? 

#The driver will automatically attach the current time of your host computer right before transmitting the bytes.

cd mars_mvp/message_driver/app

# 1. Force-stop and remove any previously active message-driver-stream container instance
sudo docker stop message-driver-stream 2>/dev/null
sudo docker rm message-driver-stream 2>/dev/null

# 2. Compile your Java source code and bundle the executable payload jar artifact
../gradlew clean build -x test

# 3. Build your working distroless container image layer locally
sudo docker build -t mars-message-driver .

# 4. Launch the container standalone with host volume mapping and network alias linking
sudo docker run -it --rm \
  --name message-driver-stream \
  --network mars-pipeline-net \
  --network-alias localhost \
  --network-alias 127.0.0.1 \
  -v "/home/hhill/Workspace/java_projects/mars_mvp/message_driver/app/data_input_files:/app/data_input_files" \
  mars-message-driver

#to run in file mode
# Finalized Standalone File Mode Generation Command:
sudo docker run -it --rm \
  -v "/home/hhill/Workspace/java_projects/mars_mvp/message_driver/app/data_input_files:/data_input_files" \
  mars-message-driver --file-mode

-------------------------------------------------------------------------------------------------
		Running and building the transition engine (transition_engine gradle project)
-------------------------------------------------------------------------------------------------
# how cache works
#The In-Memory Map (affinityCache): A fast ConcurrentHashMap that allows for lightning-fast network routing checks.
#The Disk State Database (routing_state_map.db): An append-only persistent storage file.

#how to scale to utilize more containers java code
// Example: Scaled seamlessly from 3 containers to 5 containers
private static final String[] CONTAINER_HOSTS = {"mars-server-1", "mars-server-2", "mars-server-3", "mars-server-4", "mars-server-5"};
private static final int[] CONTAINER_PORTS = {12346, 12347, 12348, 12349, 12340}; 
private static final int TOTAL_CONTAINERS = CONTAINER_PORTS.length;

#cd into the transition_engine app folder from the mars_mvp folder
cd transition_engine/app
# 1. Stop and remove any old engine containers from Docker's memory cache
sudo docker stop transition-engine-server 2>/dev/null
sudo docker rm transition-engine-server 2>/dev/null
# 2. Compile and package the core routing engine binary payload
../gradlew clean build -x test
# 3. Rebuild the Distroless Engine Docker image
sudo docker build -t mars-transition-engine .
# 4. Launch the central engine routing middleware with console logs attached (with auto-cleanup --rm)
# Optimized Step 4 for cluster-wide communication:
sudo docker run -it --rm \
  --name transition-engine-server \
  --network mars-pipeline-net \
  --network-alias transition-engine-server \
  -p 12345:12345 \
  mars-transition-engine

-------------------------------------------------------------------------------------------------
		Running and building the containers (mars_container gradle project)
-------------------------------------------------------------------------------------------------
#cd into the app folder within the mars_container gradle project
cd mars_container/app/
# 1. Stop and remove any leftover container names from Docker's memory cache
sudo docker stop mars-server-1 mars-server-2 mars-server-3 2>/dev/null
sudo docker rm mars-server-1 mars-server-2 mars-server-3 2>/dev/null
# 2. Compile and package the updated application binary payload
../gradlew clean build -x test
# 3. Rebuild the immutable Distroless Docker image
sudo docker build -t universal-mars-container .
# 4. Launch the 3 load-balanced container workers across independent ports (with auto-cleanup --rm)
sudo docker run -it --rm --name mars-server-1 --network mars-pipeline-net --network-alias mars-server-1 -p 12346:12346 universal-mars-container 12346

sudo docker run -it --rm --name mars-server-2 --network mars-pipeline-net --network-alias mars-server-2 -p 12347:12347 universal-mars-container 12347

sudo docker run -it --rm --name mars-server-3 --network mars-pipeline-net --network-alias mars-server-3 -p 12348:12348 universal-mars-container 12348

-------------------------------------------------------------------------------------------------
		Running and building the final publisher
-------------------------------------------------------------------------------------------------
#cd into the publisher app folder
cd publisher/app
# 1. Force-stop and remove any previously active publisher container instance
sudo docker stop publisher-server 2>/dev/null
sudo docker rm publisher-server 2>/dev/null

# 2. Compile your Java source code and bundle the executable dashboard jar artifact
../gradlew clean build -x test

# 3. Build your working distroless publisher image layer locally
sudo docker build -t mars-publisher .

# 4. Launch the dashboard server standalone with explicit port mapping and network name aliases
sudo docker run -it --rm \
  --name publisher-server \
  --network mars-pipeline-net \
  --network-alias reporting-publisher-server \
  -p 12349:12349 \
  mars-publisher

-------------------------------------------------------------------------------------------------
			Starting and querying the Database
-------------------------------------------------------------------------------------------------
#login to oracle user
sudo docker exec -it oracle-enterprise-db sqlplus system/password123@//localhost:1521/ORCLCDB

#reset tables
ALTER SESSION SET CONTAINER = ORCLCDB;
TRUNCATE TABLE event_messages;
TRUNCATE TABLE app_events;

# show events and all information in one query
SET PAGESIZE 100;
SET LINESIZE 250;
COLUMN event_id FORMAT 999999;
COLUMN track_id FORMAT A15;
COLUMN total_messages FORMAT 99999999;
COLUMN unique_sources_count FORMAT 99999999;
COLUMN corporate_sources_list FORMAT A45;

SELECT 
    p.event_id,
    p.track_id,
    COUNT(c.message_id) AS total_messages,
    COUNT(DISTINCT c.source) AS unique_sources_count,
    LISTAGG(DISTINCT c.source, ', ') WITHIN GROUP (ORDER BY c.source) AS corporate_sources_list
FROM 
    app_events p
JOIN 
    event_messages c ON p.event_id = c.event_id
GROUP BY 
    p.event_id, p.track_id
HAVING 
    COUNT(DISTINCT c.source) > 1  -- Filters focus down to track groups holding DIFFERENT sources
ORDER BY 
    unique_sources_count DESC, total_messages DESC, p.event_id ASC;








