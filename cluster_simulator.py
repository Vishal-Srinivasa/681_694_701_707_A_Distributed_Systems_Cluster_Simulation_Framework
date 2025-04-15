# cluster_simulator.py
from flask import Flask, request, jsonify
from flask_cors import CORS # Import CORS
import docker
import uuid
import logging # Keep existing import
import threading
import time
import os
import random
import json # Ensure json is imported if needed for logging payloads
from typing import Dict, Any, List, Optional

# --- Main Logger Setup (Existing - DO NOT CHANGE) ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s", # Existing format
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="cluster_simulator.log", # Existing main log file
    filemode="a"
)
# Existing main logger instance
logger = logging.getLogger(__name__) # This is the main logger

werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.ERROR)
werkzeug_logger.handlers = []

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
# Use existing formatter style for console consistency or define a new one
formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler) # Add console to main logger


# --- *** NEW: Heartbeat Logger Setup *** ---
heartbeat_logger = logging.getLogger('HeartbeatLogger')
heartbeat_logger.setLevel(logging.INFO) # Log INFO level messages and above for heartbeats
# Prevent heartbeat logs from propagating to the root logger/main handlers
heartbeat_logger.propagate = False

# Create file handler for the heartbeat log
heartbeat_file_handler = logging.FileHandler('heartbeats.log', mode='a') # New log file name

# Create formatter for the heartbeat log (can be same or different)
heartbeat_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
heartbeat_file_handler.setFormatter(heartbeat_formatter)

# Add the handler to the heartbeat logger
heartbeat_logger.addHandler(heartbeat_file_handler)
# --- *** End of Heartbeat Logger Setup *** ---


app = Flask(__name__)
CORS(app) # Initialize CORS for the entire app

try:
    client = docker.from_env()
except docker.errors.DockerException as e:
    # Log to main logger
    logger.error(f"Failed to connect to Docker daemon: {e}")
    logger.error("Please ensure Docker is running and accessible.")
    exit(1)

# --- Constants (Keep as is) ---
CONTAINER_BASE_IMAGE = "python:3.9-slim"
CONTAINER_NAME_PREFIX = "node-"
CPU_UNITS_PER_CORE = 100000
HEARTBEAT_INTERVAL_SECONDS = 10
HEARTBEAT_TIMEOUT_SECONDS = 35
HEALTH_CHECK_INTERVAL_SECONDS = 15

# --- Global State (Keep as is) ---
nodes: Dict[str, Dict[str, Any]] = {}
pods: Dict[str, Dict[str, Any]] = {}
current_node_number = 0
nodes_lock = threading.Lock()
pods_lock = threading.Lock()
node_number_lock = threading.Lock()

# --- Helper Functions (Keep as is) ---
def generate_node_name() -> str:
    global current_node_number
    with node_number_lock:
        current_node_number += 1
        return f"{CONTAINER_NAME_PREFIX}{current_node_number}"

# --- API Endpoints ---

@app.route("/nodes/add", methods=["POST"])
def add_node():
    # --- (Code remains the same as previous version) ---
    # ... (no changes needed here for logging)
    try:
        data = request.json
        if not data: return jsonify({"error": "Missing JSON payload"}), 400
        cpu_cores = data.get("cpu_cores")
        if not cpu_cores or not isinstance(cpu_cores, (int, float)) or cpu_cores <= 0:
            return jsonify({"error": "Invalid or missing 'cpu_cores'. Must be a positive number."}), 400

        node_id = str(uuid.uuid4())
        node_name = generate_node_name()
        cpu_quota = int(cpu_cores * CPU_UNITS_PER_CORE)

        agent_script_path = os.path.abspath("node_agent.py")
        if not os.path.exists(agent_script_path):
             logger.error("node_agent.py not found.") # Log error to main logger
             return jsonify({"error": "Node agent script not found."}), 500

        api_server_url = "http://host.docker.internal:5000" # Adjust if needed

        container_command = [
            "sh", "-c",
            "pip install requests > /dev/null 2>&1 && python /app/node_agent.py"
        ]

        container = client.containers.run(
            image=CONTAINER_BASE_IMAGE,
            command=container_command,
            detach=True,
            cpu_period=CPU_UNITS_PER_CORE,
            cpu_quota=cpu_quota,
            name=node_name,
            hostname=node_name,
            volumes={ agent_script_path: {'bind': '/app/node_agent.py', 'mode': 'ro'} },
            environment={ # Pass necessary info AND simulation params
                "NODE_ID": node_id,
                "API_SERVER_URL": api_server_url,
                "HEARTBEAT_INTERVAL": str(HEARTBEAT_INTERVAL_SECONDS),
            },
            remove=True,
        )

        with nodes_lock:
            nodes[node_id] = {
                "node_id": node_id, "node_name": node_name, "cpu_cores": cpu_cores,
                "available_cpu": cpu_cores, "container_id": container.id,
                "status": "Ready", "last_heartbeat_time": time.time(), "pods": []
            }
        logger.info(f"Node added: {node_name} (ID: {node_id}) with {cpu_cores} CPU cores.") # Log to main logger
        return jsonify({
            "message": "Node added successfully", "node_id": node_id,
            "node_name": node_name, "cpu_cores": cpu_cores
        }), 201

    except docker.errors.APIError as docker_api_error:
        logger.error(f"Docker API error adding node: {str(docker_api_error)}") # Log to main logger
        try:
            existing_container = client.containers.get(node_name)
            existing_container.remove(force=True)
        except: pass
        return jsonify({"error": "Failed to add node due to Docker error", "details": str(docker_api_error)}), 500
    except Exception as e:
        logger.exception(f"Unexpected error adding node: {str(e)}") # Log to main logger
        return jsonify({"error": "Failed to add node", "details": str(e)}), 500


@app.route("/nodes/heartbeat/<node_id>", methods=["POST"])
def node_heartbeat(node_id):
    """
    Receives heartbeat signals AND pod statuses from nodes.
    Updates node's last heartbeat time.
    Processes reported pod failures, triggers rescheduling.
    Responds with the authoritative list of pods assigned to this node.
    *** Logs heartbeat events to a separate file (heartbeats.log). ***
    """
    pods_to_reschedule_now = []
    current_server_assigned_pods = []
    node_name = f"unknown ({node_id})"
    data = request.json or {} # Get payload early for logging

    # --- Log Heartbeat Reception to BOTH loggers ---
    # Log concise message to main logger (optional, could remove if too noisy)
    logger.debug(f"Heartbeat received from Node ID {node_id}. Payload: {data}")
    # Log detailed message to dedicated heartbeat logger
    # We fetch node_name later, so log with ID first
    heartbeat_logger.info(f"Received from Node ID {node_id}. Payload: {json.dumps(data)}") # Log full payload as JSON string


    # 1. --- Basic Node Heartbeat Update ---
    with nodes_lock:
        if node_id not in nodes:
            # Log warning to both
            logger.warning(f"Heartbeat received from unknown/removed node: {node_id}")
            heartbeat_logger.warning(f"Heartbeat from unknown/removed node: {node_id}")
            return jsonify({"error": f"Node {node_id} not found"}), 404

        node_info = nodes[node_id]
        node_name = node_info.get("node_name", node_name) # Get actual name
        current_time = time.time()
        time_since_last = current_time - node_info.get("last_heartbeat_time", current_time)
        node_info["last_heartbeat_time"] = current_time

        # Update heartbeat logger message with name if desired (or keep separate)
        # heartbeat_logger.info(f"Heartbeat processed for {node_name} (ID: {node_id}). Time since last: {time_since_last:.2f}s")

        # Recover node if it was NotReady
        if node_info["status"] == "NotReady":
            node_info["status"] = "Ready"
            # Log recovery to both loggers
            logger.info(f"Node {node_name} (ID: {node_id}) recovered via heartbeat.")
            heartbeat_logger.info(f"Node {node_name} (ID: {node_id}) recovered via this heartbeat.")

        current_server_assigned_pods = node_info["pods"].copy()

    # 2. --- Process Reported Pod Statuses ---
    # data = request.json or {} # Moved up
    reported_pod_statuses = data.get("pod_statuses", {})
    # Existing main logger debug message (already present)
    # logger.debug(f"Heartbeat received from {node_name} (ID: {node_id}). Time since last: {time_since_last:.2f}s. Reported statuses: {reported_pod_statuses}")

    if reported_pod_statuses:
        pods_reported_failed = []

        with pods_lock:
            for pod_id, reported_status in reported_pod_statuses.items():
                if reported_status == "Failed":
                    if pod_id in pods and pods[pod_id]["node_id"] == node_id and pods[pod_id]["status"] == "Running":
                        pod_info = pods[pod_id]
                        cpu_request = pod_info["cpu_request"]
                        # Log failure report processing to both loggers
                        msg = f"Node {node_name} reported Pod {pod_id} (CPU: {cpu_request}) as Failed. Marking as Pending."
                        logger.warning(msg) # Main logger
                        heartbeat_logger.warning(f"Pod failure reported: Node={node_name}({node_id}), Pod={pod_id}, CPU={cpu_request}. Marking Pending.") # Heartbeat logger

                        pod_info["status"] = "Pending"; pod_info["node_id"] = None
                        pods_reported_failed.append({"id": pod_id, "cpu": cpu_request})

        # 3. --- Release Resources for Reported Failed Pods ---
        if pods_reported_failed:
            with nodes_lock:
                if node_id in nodes:
                    node_info = nodes[node_id]
                    for failed_pod_data in pods_reported_failed:
                        pod_id_to_remove = failed_pod_data["id"]
                        cpu_to_release = failed_pod_data["cpu"]
                        if pod_id_to_remove in node_info["pods"]:
                            node_info["pods"].remove(pod_id_to_remove)
                            node_info["available_cpu"] += cpu_to_release
                            # Log resource release to both loggers
                            msg = f"Released {cpu_to_release} CPU from Node {node_name} for reported failed Pod {pod_id_to_remove}."
                            logger.info(msg) # Main logger
                            heartbeat_logger.info(f"Resource release: Node={node_name}({node_id}), Pod={pod_id_to_remove}, CPU={cpu_to_release}") # Heartbeat logger
                            pods_to_reschedule_now.append(pod_id_to_remove)
                        else: # Log warning to both
                            logger.warning(f"Pod {pod_id_to_remove} reported failed by {node_name} but not found in its list.")
                            heartbeat_logger.warning(f"Pod {pod_id_to_remove} reported failed by {node_name} but not in node's list during resource release.")
                else: # Log warning to both
                     logger.warning(f"Node {node_name} removed while processing reported pod failures.")
                     heartbeat_logger.warning(f"Node {node_name}({node_id}) removed during pod failure processing.")

    # 4. --- Trigger Rescheduling (if any pods marked Pending) ---
    if pods_to_reschedule_now:
        # Log trigger to main logger (already present)
        logger.info(f"Triggering reschedule check for {len(pods_to_reschedule_now)} pods due to reported failures on Node {node_name}.")
        # Log trigger also to heartbeat logger
        heartbeat_logger.info(f"Triggering reschedule for {len(pods_to_reschedule_now)} pods from {node_name} due to reported failures: {pods_to_reschedule_now}")

        reschedule_results = reschedule_pods(pods_to_reschedule_now) # Handles locking

        # Log completion to main logger (already present)
        logger.info(f"Rescheduling from reported failures on {node_name} completed. Success: {len(reschedule_results['success'])}, Failed: {len(reschedule_results['failed'])}")
         # Log completion also to heartbeat logger
        heartbeat_logger.info(f"Reschedule from reported failures on {node_name} done. Success: {len(reschedule_results['success'])}, Failed: {len(reschedule_results['failed'])}")


    # 5. --- Respond with Current Pod Assignment ---
    with nodes_lock:
        final_assigned_pods = nodes.get(node_id, {}).get("pods", [])

    # Log response being sent to heartbeat logger
    heartbeat_logger.debug(f"Responding to heartbeat from {node_name}({node_id}) with assigned_pods: {final_assigned_pods}")

    return jsonify({
        "status": "acknowledged",
        "assigned_pods": final_assigned_pods
        }), 200


# --- Other Endpoints ---
# No changes needed in the logic of other endpoints for this logging requirement.
# They will continue to log to the main logger as before.

@app.route("/nodes/list", methods=["GET"])
def list_nodes():
    # (Code remains the same)
    # ...
    try:
        active_nodes_list = []
        with nodes_lock:
            for node_id, node_info in nodes.items():
                if node_info["status"] == "Ready":
                    assigned_pods_details = []
                    with pods_lock:
                        for pod_id in node_info["pods"]:
                            if pod_id in pods:
                                assigned_pods_details.append({
                                    "pod_id": pod_id,
                                    "cpu_request": pods[pod_id]["cpu_request"]
                                })
                            else: logger.warning(f"Node {node_info['node_name']} has ref to non-existent pod {pod_id}")
                    active_nodes_list.append({
                        "node_id": node_id, "node_name": node_info["node_name"],
                        "cpu_cores": node_info["cpu_cores"], "available_cpu": node_info["available_cpu"],
                        "container_id": node_info["container_id"], "status": node_info["status"],
                        "last_heartbeat_time": node_info.get("last_heartbeat_time"),
                        "pods": assigned_pods_details
                    })
        return jsonify({"nodes": active_nodes_list}), 200
    except Exception as e:
        logger.exception(f"Error listing nodes: {str(e)}")
        return jsonify({"error": "Failed to list nodes", "details": str(e)}), 500

@app.route("/nodes/remove/<node_id>", methods=["DELETE"])
def remove_node(node_id):
    # (Code remains the same)
    # ...
    node_info = None
    node_name = f"unknown ({node_id})"
    orphaned_pods_ids = []
    try:
        with nodes_lock:
            if node_id not in nodes: return jsonify({"error": f"Node {node_id} not found"}), 404
            node_info = nodes[node_id]; node_name = node_info["node_name"]
            container_id = node_info["container_id"]; orphaned_pods_ids = node_info["pods"].copy()
            del nodes[node_id]
            logger.info(f"Removed node {node_name} (ID: {node_id}) from internal state.")
        try:
            container = client.containers.get(container_id)
            logger.info(f"Stopping container {container_id} for node {node_name}...")
            container.stop(timeout=5)
            logger.info(f"Container {container_id} for node {node_name} stopped.")
        except docker.errors.NotFound: logger.warning(f"Container {container_id} for node {node_name} not found during removal.")
        except Exception as container_e: logger.error(f"Error stopping/removing container {container_id} for node {node_name}: {str(container_e)}")
        pods_to_reschedule = []
        with pods_lock:
            for pod_id in orphaned_pods_ids:
                if pod_id in pods:
                    pods[pod_id]["node_id"] = None; pods[pod_id]["status"] = "Pending"
                    pods_to_reschedule.append(pod_id)
                    logger.info(f"Marked pod {pod_id} from removed node {node_name} as Pending.")
                else: logger.warning(f"Pod {pod_id} listed on removed node {node_name} was not found.")
        reschedule_result = {}
        if pods_to_reschedule:
            logger.info(f"Attempting reschedule for {len(pods_to_reschedule)} pods from removed node {node_name}...")
            reschedule_result = reschedule_pods(pods_to_reschedule)
        return jsonify({"message": f"Node {node_name} removed", "orphaned_pods": pods_to_reschedule, "reschedule_status": reschedule_result}), 200
    except Exception as e:
        logger.exception(f"Error removing node {node_name}: {str(e)}")
        with nodes_lock: node_still_exists = node_id in nodes
        return jsonify({"error": f"Failed to remove node {node_name}", "details": str(e), "node_removed_from_state": not node_still_exists}), 500

@app.route("/pods/launch", methods=["POST"])
def launch_pod():
    # (Code remains the same)
    # ...
    try:
        data = request.json
        if not data: return jsonify({"error": "Missing JSON payload"}), 400
        cpu_request = data.get("cpu_request")
        if not cpu_request or not isinstance(cpu_request, (int, float)) or cpu_request <= 0:
            return jsonify({"error": "Invalid 'cpu_request'."}), 400
        pod_id = str(uuid.uuid4())
        selected_node_id = best_fit_scheduler(cpu_request)
        if not selected_node_id:
            with pods_lock: pods[pod_id] = {"pod_id": pod_id, "cpu_request": cpu_request, "node_id": None, "status": "Pending", "created_at": time.time()}
            logger.info(f"Pod {pod_id} created (CPU: {cpu_request}) but no node found. Status: Pending.")
            return jsonify({"message": "Pod created but pending resources", "pod_id": pod_id, "status": "Pending", "cpu_request": cpu_request}), 202
        else:
            node_name = "Unknown"
            with nodes_lock:
                if selected_node_id not in nodes or nodes[selected_node_id]["status"] != "Ready":
                     logger.warning(f"Node {selected_node_id} for pod {pod_id} unavailable before assignment.")
                     with pods_lock: pods[pod_id] = {"pod_id": pod_id, "cpu_request": cpu_request, "node_id": None, "status": "Pending", "created_at": time.time()}
                     return jsonify({"message": "Pod created but selected node unavailable", "pod_id": pod_id, "status": "Pending", "cpu_request": cpu_request}), 202
                node = nodes[selected_node_id]; node["available_cpu"] -= cpu_request
                node["pods"].append(pod_id); node_name = node["node_name"]
            with pods_lock: pods[pod_id] = {"pod_id": pod_id, "cpu_request": cpu_request, "node_id": selected_node_id, "status": "Running", "created_at": time.time()}
            logger.info(f"Pod {pod_id} (CPU: {cpu_request}) launched on node {node_name} (ID: {selected_node_id}).")
            return jsonify({"message": "Pod launched successfully", "pod_id": pod_id, "node_id": selected_node_id, "node_name": node_name, "cpu_request": cpu_request}), 201
    except Exception as e:
        logger.exception(f"Error launching pod: {str(e)}")
        return jsonify({"error": "Failed to launch pod", "details": str(e)}), 500

@app.route("/pods/list", methods=["GET"])
def list_pods():
    # (Code remains the same)
    # ...
    try:
        pod_list = []
        with pods_lock: pod_items = list(pods.items())
        with nodes_lock: node_names = {nid: ninfo["node_name"] for nid, ninfo in nodes.items()}
        for pod_id, pod_info in pod_items:
            node_name = node_names.get(pod_info["node_id"]) if pod_info["node_id"] else None
            if pod_info["node_id"] and not node_name: logger.warning(f"Pod {pod_id} refs node {pod_info['node_id']} not in node list.")
            pod_list.append({"pod_id": pod_id, "cpu_request": pod_info["cpu_request"], "node_id": pod_info["node_id"], "node_name": node_name, "status": pod_info["status"], "created_at": pod_info.get("created_at", "N/A")})
        return jsonify({"pods": pod_list}), 200
    except Exception as e:
        logger.exception(f"Error listing pods: {str(e)}")
        return jsonify({"error": "Failed to list pods", "details": str(e)}), 500

@app.route("/pods/delete/<pod_id>", methods=["DELETE"])
def delete_pod(pod_id):
    # (Code remains the same)
    # ...
    try:
        pod_info = None
        with pods_lock:
            if pod_id not in pods: return jsonify({"error": f"Pod {pod_id} not found"}), 404
            pod_info = pods[pod_id]; del pods[pod_id]
            logger.info(f"Pod {pod_id} removed from central pod store.")
        node_id = pod_info.get("node_id")
        if node_id:
            with nodes_lock:
                if node_id in nodes:
                    node = nodes[node_id]
                    if pod_id in node["pods"]:
                        node["pods"].remove(pod_id)
                        cpu_request = pod_info.get("cpu_request", 0)
                        if isinstance(cpu_request, (int, float)) and cpu_request > 0:
                             node["available_cpu"] += cpu_request
                             logger.info(f"Released {cpu_request} CPU from node {node['node_name']} for deleted pod {pod_id}.")
                        else: logger.warning(f"Invalid cpu_request ({cpu_request}) for deleted pod {pod_id}.")
                    else: logger.warning(f"Pod {pod_id} assigned to node {node['node_name']} but not in its pod list.")
                else: logger.warning(f"Node {node_id} for deleted pod {pod_id} not found.")
        return jsonify({"message": f"Pod {pod_id} deleted successfully"}), 200
    except Exception as e:
        logger.exception(f"Error deleting pod {pod_id}: {str(e)}")
        with pods_lock: pod_still_exists = pod_id in pods
        return jsonify({"error": f"Failed to delete pod {pod_id}", "details": str(e), "pod_removed_from_state": not pod_still_exists}), 500

@app.route("/health", methods=["GET"])
def health_check():
    # (Code remains the same)
    # ...
    with nodes_lock: node_count = len(nodes); ready_nodes = sum(1 for n in nodes.values() if n['status'] == 'Ready')
    with pods_lock: pod_count = len(pods); running_pods = sum(1 for p in pods.values() if p['status'] == 'Running'); pending_pods = sum(1 for p in pods.values() if p['status'] == 'Pending')
    return jsonify({"status": "healthy", "node_count_total": node_count, "node_count_ready": ready_nodes, "pod_count_total": pod_count, "pod_count_running": running_pods, "pod_count_pending": pending_pods, "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')}), 200


# --- Scheduling and Health Monitoring Logic ---

def best_fit_scheduler(cpu_request: float) -> Optional[str]:
    # (Code remains the same)
    # ...
    best_node_id = None; smallest_sufficient_capacity_remaining = float('inf')
    with nodes_lock:
        candidates = []
        for node_id, node_info in nodes.items():
            if node_info["status"] == "Ready":
                available_cpu = node_info["available_cpu"]
                if available_cpu >= cpu_request:
                    remaining_capacity = available_cpu - cpu_request
                    candidates.append((remaining_capacity, node_id))
        if candidates: candidates.sort(); best_node_id = candidates[0][1]
    return best_node_id

def reschedule_pods(pod_ids: List[str]) -> Dict[str, Any]:
    # (Code remains the same)
    # ...
    results = {"success": [], "failed": []};
    if not pod_ids: return results
    logger.info(f"Rescheduling check requested for pods: {pod_ids}") # Main logger
    for pod_id in pod_ids:
        with pods_lock:
            if pod_id not in pods: results["failed"].append({"pod_id": pod_id, "reason": "Not found"}); continue
            pod_info = pods[pod_id]
            if pod_info["status"] != "Pending": logger.debug(f"Skip reschedule: Pod {pod_id} status {pod_info['status']}"); continue
            cpu_request = pod_info["cpu_request"]
            new_node_id = best_fit_scheduler(cpu_request)
            if new_node_id:
                with nodes_lock:
                    if new_node_id not in nodes or nodes[new_node_id]["status"] != "Ready":
                         logger.warning(f"Node {new_node_id} for pod {pod_id} unavailable."); pod_info["status"] = "Pending"
                         results["failed"].append({"pod_id": pod_id, "reason": f"Selected node {new_node_id} unavailable"}); continue
                    nodes[new_node_id]["available_cpu"] -= cpu_request; nodes[new_node_id]["pods"].append(pod_id)
                    node_name = nodes[new_node_id]["node_name"]
                pod_info["node_id"] = new_node_id; pod_info["status"] = "Running"
                results["success"].append({"pod_id": pod_id, "new_node_id": new_node_id, "node_name": node_name})
                logger.info(f"Pod {pod_id} rescheduled to node {node_name} (ID: {new_node_id})") # Main logger
            else:
                pod_info["status"] = "Pending" # Keep pending
                results["failed"].append({"pod_id": pod_id, "reason": "No suitable node found"})
                logger.info(f"Pod {pod_id} could not be rescheduled: No suitable node.") # Main logger
    return results

def handle_node_failure(node_id: str, node_name: str):
    # (Code remains the same - logs to main logger)
    # ...
    logger.warning(f"Handling failure (timeout) for node {node_name} (ID: {node_id}).") # Main logger
    pods_to_reschedule = []
    with nodes_lock:
        if node_id not in nodes or nodes[node_id]["status"] != "NotReady": logger.info(f"Node {node_name} no longer NotReady. Skipping failure handling."); return
        failed_node = nodes[node_id]; pod_ids_on_node = failed_node["pods"].copy()
        failed_node["pods"] = [] # Clear pod list on failed node
    with pods_lock:
        for pod_id in pod_ids_on_node:
            if pod_id in pods:
                if pods[pod_id]["node_id"] == node_id: # Ensure it was still assigned here
                    pods[pod_id]["status"] = "Pending"; pods[pod_id]["node_id"] = None
                    pods_to_reschedule.append(pod_id)
                    logger.info(f"Marked pod {pod_id} from failed node {node_name} as Pending.") # Main logger
                else: logger.warning(f"Pod {pod_id} listed on failed node {node_name} but state points to node {pods[pod_id]['node_id']}.") # Main logger
            else: logger.warning(f"Pod {pod_id} listed on failed node {node_name} not found.") # Main logger
    if pods_to_reschedule:
        logger.info(f"Attempting reschedule for {len(pods_to_reschedule)} pods from failed node {node_name}...") # Main logger
        reschedule_results = reschedule_pods(pods_to_reschedule)
        logger.info(f"Rescheduling from failed node {node_name} completed. Success: {len(reschedule_results['success'])}, Failed: {len(reschedule_results['failed'])}") # Main logger
    else: logger.info(f"No running pods found on failed node {node_name} to reschedule.") # Main logger


def health_monitor_loop():
    # (Code remains the same - logs node timeouts to main logger)
    # ...
    logger.info("Health monitor thread started.") # Main logger
    while True:
        try:
            time.sleep(HEALTH_CHECK_INTERVAL_SECONDS)
            current_time = time.time()
            nodes_to_check = []
            with nodes_lock: nodes_to_check = list(nodes.items())
            failed_nodes_detected = []
            for node_id, node_info in nodes_to_check:
                with nodes_lock:
                    if node_id not in nodes: continue
                    node_info = nodes[node_id]; last_beat = node_info.get("last_heartbeat_time"); status = node_info["status"]
                    if status == "Ready" and last_beat:
                        time_since_last_beat = current_time - last_beat
                        if time_since_last_beat > HEARTBEAT_TIMEOUT_SECONDS:
                             logger.warning(f"Node {node_info['node_name']} missed heartbeat! Last: {time_since_last_beat:.2f}s ago.") # Main logger
                             node_info["status"] = "NotReady"; failed_nodes_detected.append((node_id, node_info['node_name']))
            if failed_nodes_detected:
                logger.info(f"Detected {len(failed_nodes_detected)} nodes as NotReady via timeout.") # Main logger
                for node_id, node_name in failed_nodes_detected: handle_node_failure(node_id, node_name)
        except Exception as e: logger.exception(f"Error in health monitor loop: {e}"); time.sleep(HEALTH_CHECK_INTERVAL_SECONDS) # Main logger

def initialize_app():
    # (Code remains the same - logs to main logger)
    # ...
    global current_node_number; logger.info("Initializing Cluster Simulator...") # Main logger
    try:
        client.ping(); logger.info("Docker connection successful.") # Main logger
        existing_containers = client.containers.list(all=True, filters={"name": f"^{CONTAINER_NAME_PREFIX}"})
        highest_number = 0
        for container in existing_containers:
            name = container.name
            if name.startswith(CONTAINER_NAME_PREFIX):
                try: number = int(name[len(CONTAINER_NAME_PREFIX):]); highest_number = max(highest_number, number)
                except ValueError: pass
        with node_number_lock: current_node_number = highest_number
        logger.info(f"Initialized node counter start: {current_node_number+1}") # Main logger
    except Exception as e: logger.exception(f"Error during initialization: {str(e)}") # Main logger
    logger.info("Initialization complete.") # Main logger


# --- Main Execution ---
if __name__ == "__main__":
    initialize_app()
    health_thread = threading.Thread(target=health_monitor_loop, name="HealthMonitor", daemon=True)
    health_thread.start()
    logger.info("Starting Flask application server...") # Main logger
    # Log confirmation that heartbeat logger is set up
    heartbeat_logger.info("Heartbeat logger initialized. Logging to heartbeats.log")

    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)