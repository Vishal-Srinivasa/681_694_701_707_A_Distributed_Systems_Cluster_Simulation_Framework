# node_agent.py
import os
import time
import requests
import logging
import sys
import random # Added
import json # Added

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout # Log to container stdout
)
logger = logging.getLogger("NodeAgent")

# --- Configuration (from Environment Variables) ---
NODE_ID = os.environ.get("NODE_ID")
API_SERVER_URL = os.environ.get("API_SERVER_URL")
try:
    HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "10")) # Default 10s
except ValueError:
    logger.error("Invalid HEARTBEAT_INTERVAL environment variable. Using default (10s).")
    HEARTBEAT_INTERVAL = 10

# --- Simulation Parameters ---
# Use environment variables or defaults for failure rates
try:
    POD_FAILURE_RATE = float(os.environ.get("POD_FAILURE_RATE", "0.02")) # Default 2%
    # NODE_CRASH_RATE = float(os.environ.get("NODE_CRASH_RATE", "0.001")) # Optional: Default 0.1%
    NODE_CRASH_RATE = 0.0 # Disable node crash simulation for now to focus on pod failure
except ValueError:
    logger.error("Invalid failure rate environment variable. Using defaults.")
    POD_FAILURE_RATE = 0.02
    NODE_CRASH_RATE = 0.0

# --- Validation ---
if not NODE_ID:
    logger.error("FATAL: NODE_ID environment variable not set.")
    sys.exit(1)
if not API_SERVER_URL:
    logger.error("FATAL: API_SERVER_URL environment variable not set.")
    sys.exit(1)

HEARTBEAT_ENDPOINT = f"{API_SERVER_URL}/nodes/heartbeat/{NODE_ID}"
logger.info(f"Node Agent starting for Node ID: {NODE_ID}")
logger.info(f"API Server URL: {API_SERVER_URL}")
logger.info(f"Heartbeat Interval: {HEARTBEAT_INTERVAL} seconds")
logger.info(f"Heartbeat Endpoint: {HEARTBEAT_ENDPOINT}")
logger.info(f"Pod Failure Rate: {POD_FAILURE_RATE*100:.2f}%")
if NODE_CRASH_RATE > 0:
    logger.info(f"Node Crash Rate: {NODE_CRASH_RATE*100:.2f}%")


# --- Agent State ---
# Stores the set of pod IDs the agent believes it should be running,
# based on the last successful response from the API server.
current_assigned_pods = set()

# --- Main Loop ---
def run_agent():
    """Main loop for sending heartbeats and simulating status."""
    global current_assigned_pods
    while True:
        # 1. Simulate Node Crash (Optional)
        if NODE_CRASH_RATE > 0 and random.random() < NODE_CRASH_RATE:
            logger.warning(f"--- Simulating Node Crash for Node {NODE_ID}! ---")
            sys.exit(1) # Exit abruptly

        # 2. Simulate Pod Statuses
        pod_statuses = {}
        if current_assigned_pods:
            logger.debug(f"Simulating status for pods: {current_assigned_pods}")
            for pod_id in current_assigned_pods:
                # Simulate failure randomly
                if random.random() < POD_FAILURE_RATE:
                    pod_statuses[pod_id] = "Failed"
                    logger.warning(f"Simulating Pod {pod_id} status as Failed for next heartbeat.")
                else:
                    pod_statuses[pod_id] = "Running"
                    # logger.debug(f"Simulating Pod {pod_id} status as Running.") # Verbose
        else:
            logger.debug("No pods currently assigned by server to simulate.")


        # 3. Prepare Heartbeat Payload
        payload = {
            "timestamp": time.time(),
            "pod_statuses": pod_statuses
        }

        # 4. Send Heartbeat
        try:
            response = requests.post(HEARTBEAT_ENDPOINT, json=payload, timeout=5) # Add timeout
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            logger.info(f"Heartbeat sent successfully. Status: {response.status_code}. Payload: {payload}")

            # 5. Process Response - Update assigned pods list
            try:
                response_data = response.json()
                server_assigned_pods = set(response_data.get("assigned_pods", []))

                if server_assigned_pods != current_assigned_pods:
                    logger.info(f"Received updated pod assignment from server: {server_assigned_pods} (previously {current_assigned_pods})")
                    current_assigned_pods = server_assigned_pods
                else:
                    logger.debug(f"Pod assignment confirmed by server: {current_assigned_pods}")

            except json.JSONDecodeError:
                logger.error("Failed to decode JSON response from server.")
            except Exception as resp_e:
                 logger.error(f"Error processing server response: {resp_e}")


        except requests.exceptions.Timeout:
            logger.warning(f"Heartbeat request timed out to {HEARTBEAT_ENDPOINT}")
            # Don't update assigned pods if request failed
        except requests.exceptions.ConnectionError:
            logger.warning(f"Heartbeat failed: Could not connect to API server at {API_SERVER_URL}")
            # Don't update assigned pods if request failed
        except requests.exceptions.RequestException as e:
            logger.error(f"Heartbeat failed: An error occurred: {e}")
            # Log response body if available and helpful
            if e.response is not None:
                logger.error(f"Response status: {e.response.status_code}, Body: {e.response.text[:200]}") # Log first 200 chars
            # Don't update assigned pods if request failed

        # 6. Sleep
        time.sleep(HEARTBEAT_INTERVAL)


if __name__ == "__main__":
    logger.info("Starting agent loop...")
    run_agent() # Start the main loop