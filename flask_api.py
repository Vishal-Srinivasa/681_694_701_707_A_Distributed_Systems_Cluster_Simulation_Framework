from flask import Flask, request, jsonify
import docker
import uuid
import logging
import threading
import time
from typing import Dict, Any, List, Optional


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="cluster_simulator.log",
    filemode="a"
)
logger = logging.getLogger(__name__)


werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.ERROR)  
werkzeug_logger.handlers = []  


console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
werkzeug_logger.addHandler(console_handler)

app = Flask(__name__)
client = docker.from_env()

CONTAINER_BASE_IMAGE = "archlinux"
CONTAINER_NAME_PREFIX = "node-"
CPU_UNITS_PER_CORE = 100000

# Registered nodes
nodes: Dict[str, Dict[str, Any]] = {}

# Store pods
pods: Dict[str, Dict[str, Any]] = {}

# Current highest node number
current_node_number = 0


def generate_node_name() -> str:
    """Generate a sequential node name (node-1, node-2, etc.)"""
    global current_node_number
    current_node_number += 1
    return f"{CONTAINER_NAME_PREFIX}{current_node_number}"


@app.route("/nodes/add", methods=["POST"])
def add_node():
    """
    Adds a new node by launching a Docker container with CPU constraints.
    """
    try:
        # Parse and validate request data
        data = request.json
        if not data:
            return jsonify({"error": "Missing JSON payload"}), 400
            
        cpu_cores = data.get("cpu_cores")
        if not cpu_cores or not isinstance(cpu_cores, (int, float)) or cpu_cores <= 0:
            return jsonify({"error": "Invalid or missing 'cpu_cores'. Must be a positive number."}), 400

        # Generate unique node ID and sequential container name
        node_id = str(uuid.uuid4())
        node_name = generate_node_name()
        
        # Calculate CPU quota (Docker uses CPU_UNITS_PER_CORE units per core)
        cpu_quota = int(cpu_cores * CPU_UNITS_PER_CORE)

        # Launch Docker container
        container = client.containers.run(
            image=CONTAINER_BASE_IMAGE,
            command=["sleep", "infinity"],  # Keep container alive
            detach=True,
            cpu_quota=cpu_quota,
            name=node_name
        )

        # Store node information
        nodes[node_id] = {
            "node_id": node_id,
            "node_name": node_name,
            "cpu_cores": cpu_cores,
            "available_cpu": cpu_cores,  # Initially, all CPU is available
            "container_id": container.id,
            "status": "Ready",
            "pods": []  # List to store pod IDs assigned to this node
        }

        # Log important event: Node added
        logger.info(f"Node added: {node_name} with {cpu_cores} CPU cores")
        
        # Return success response with node details
        return jsonify({
            "message": "Node added successfully",
            "node_id": node_id,
            "node_name": node_name,
            "cpu_cores": cpu_cores
        }), 200

    except Exception as e:
        logger.error(f"Error adding node: {str(e)}")
        return jsonify({"error": "Failed to add node", "details": str(e)}), 500


@app.route("/nodes/list", methods=["GET"])
def list_nodes():
    """
    Returns a list of only active nodes and their details.
    """
    try:
        # Refresh container status from Docker engine
        active_containers = {c.id: c for c in client.containers.list(all=True)}
        
        # Build response with detailed node information - only show running containers
        node_list = []
        nodes_to_update = []  # Track nodes that need status updates
        
        for node_id, node_info in nodes.items():
            container_id = node_info["container_id"]
            
            # Check if container still exists and its actual status
            if container_id in active_containers:
                container = active_containers[container_id]
                container_status = container.status
                
                # If container is not running, mark node as NotReady but don't display it
                if container_status != "running":
                    if node_info["status"] == "Ready":
                        node_info["status"] = "NotReady"
                        nodes_to_update.append(node_id)
                    # Skip non-running containers in the response
                    continue
                elif node_info["status"] != "Ready":
                    # Container is running but node was marked as NotReady
                    node_info["status"] = "Ready"
            else:
                # Container no longer exists - don't display it
                if node_info["status"] != "NotReady":
                    node_info["status"] = "NotReady"
                    nodes_to_update.append(node_id)
                # Skip deleted containers in the response
                continue
            
            # Add pod IDs assigned to this node
            assigned_pods = []
            for pod_id in node_info["pods"]:
                if pod_id in pods:
                    assigned_pods.append({
                        "pod_id": pod_id,
                        "cpu_request": pods[pod_id]["cpu_request"]
                    })
                
            node_list.append({
                "node_id": node_id,
                "node_name": node_info["node_name"],
                "cpu_cores": node_info["cpu_cores"],
                "available_cpu": node_info["available_cpu"],
                "container_id": container_id,
                "status": node_info["status"],
                "pods": assigned_pods
            })
        
        # Handle any nodes that were marked as failed
        for node_id in nodes_to_update:
            handle_node_failure(node_id)
            
        return jsonify({"nodes": node_list}), 200
        
    except Exception as e:
        logger.error(f"Error listing nodes: {str(e)}")
        return jsonify({"error": "Failed to list nodes", "details": str(e)}), 500


@app.route("/nodes/remove/<node_id>", methods=["DELETE"])
def remove_node(node_id):
    """
    Removes a node and stops its associated container.
    """
    try:
        if node_id not in nodes:
            return jsonify({"error": f"Node {node_id} not found"}), 404
            
        # Get container ID and remove container
        container_id = nodes[node_id]["container_id"]
        node_name = nodes[node_id]["node_name"]
        
        try:
            container = client.containers.get(container_id)
            container.stop()
            container.remove()
        except docker.errors.NotFound:
            pass
        except Exception as container_e:
            logger.error(f"Error stopping container for node {node_id}: {str(container_e)}")
        
        # Get pods assigned to this node for potential rescheduling
        node_pods = nodes[node_id]["pods"].copy()
        
        # Remove node from storage
        node_info = nodes.pop(node_id)
        
        # Log important event: Node deleted
        logger.info(f"Node deleted: {node_name}")
        
        # Handle orphaned pods (optionally reschedule them)
        orphaned_pods = []
        for pod_id in node_pods:
            if pod_id in pods:
                orphaned_pods.append(pod_id)
                pods[pod_id]["node_id"] = None  # Mark as unscheduled
                pods[pod_id]["status"] = "Pending"  # Mark as pending rescheduling
                
        if orphaned_pods:
            # Reschedule orphaned pods
            reschedule_result = reschedule_pods(orphaned_pods)
            
            return jsonify({
                "message": f"Node {node_id} removed successfully",
                "orphaned_pods": orphaned_pods,
                "rescheduled": reschedule_result
            }), 200
        else:
            return jsonify({
                "message": f"Node {node_id} removed successfully",
                "orphaned_pods": []
            }), 200
            
    except Exception as e:
        logger.error(f"Error removing node {node_id}: {str(e)}")
        return jsonify({"error": f"Failed to remove node {node_id}", "details": str(e)}), 500


def reschedule_pods(pod_ids: List[str]) -> Dict[str, Any]:
    """
    Attempts to reschedule a list of pods to new nodes.
    """
    results = {
        "success": [],
        "failed": []
    }
    
    for pod_id in pod_ids:
        if pod_id not in pods:
            results["failed"].append({"pod_id": pod_id, "reason": "Pod not found"})
            continue
            
        pod_info = pods[pod_id]
        cpu_request = pod_info["cpu_request"]
        
        # Find suitable node using Best-Fit algorithm
        new_node_id = best_fit_scheduler(cpu_request)
        
        if new_node_id:
            # Update node resources
            nodes[new_node_id]["available_cpu"] -= cpu_request
            nodes[new_node_id]["pods"].append(pod_id)
            
            # Update pod information
            pod_info["node_id"] = new_node_id
            pod_info["status"] = "Running"
            
            results["success"].append({
                "pod_id": pod_id, 
                "new_node_id": new_node_id,
                "node_name": nodes[new_node_id]["node_name"]
            })
            
      
            logger.info(f"Pod {pod_id} rescheduled to {nodes[new_node_id]['node_name']}")
        else:
            
            pod_info["status"] = "Pending"
            results["failed"].append({
                "pod_id": pod_id, 
                "reason": "No suitable node with enough resources"
            })
    
    return results


@app.route("/pods/launch", methods=["POST"])
def launch_pod():
    """
    Launches a new pod with specified CPU requirements.
    Uses Best-Fit scheduling algorithm to find the most suitable node.
    """
    try:
        # Parse and validate request data
        data = request.json
        if not data:
            return jsonify({"error": "Missing JSON payload"}), 400
            
        cpu_request = data.get("cpu_request")
        if not cpu_request or not isinstance(cpu_request, (int, float)) or cpu_request <= 0:
            return jsonify({"error": "Invalid or missing 'cpu_request'. Must be a positive number."}), 400

        # Generate unique pod ID
        pod_id = str(uuid.uuid4())
        
        # Find suitable node using Best-Fit algorithm
        selected_node_id = best_fit_scheduler(cpu_request)
        
        if not selected_node_id:
            # Create the pod but mark it as pending
            pods[pod_id] = {
                "pod_id": pod_id,
                "cpu_request": cpu_request,
                "node_id": None,
                "status": "Pending",
                "created_at": time.time()
            }
            
            # Log important event: Pod created but pending
            logger.info(f"Pod launched: {pod_id} (status: Pending)")
            
            return jsonify({
                "message": "Pod created but waiting for resources",
                "pod_id": pod_id,
                "status": "Pending",
                "cpu_request": cpu_request
            }), 202  # Accepted but processing not complete
        
        # Update node's available resources
        node = nodes[selected_node_id]
        node["available_cpu"] -= cpu_request
        node["pods"].append(pod_id)
        
        # Store pod information
        pods[pod_id] = {
            "pod_id": pod_id,
            "cpu_request": cpu_request,
            "node_id": selected_node_id,
            "status": "Running",
            "created_at": time.time()
        }
        
        # Log important event: Pod launched
        logger.info(f"Pod launched: {pod_id} on {node['node_name']}")
        
        # Return success response with pod details
        return jsonify({
            "message": "Pod launched successfully",
            "pod_id": pod_id,
            "node_id": selected_node_id,
            "node_name": node["node_name"],
            "cpu_request": cpu_request
        }), 200

    except Exception as e:
        logger.error(f"Error launching pod: {str(e)}")
        return jsonify({"error": "Failed to launch pod", "details": str(e)}), 500


@app.route("/pods/list", methods=["GET"])
def list_pods():
    """
    Returns a list of all pods and their details.
    """
    try:
        pod_list = []
        for pod_id, pod_info in pods.items():
            # Get node name if pod is assigned to a node
            node_name = None
            if pod_info["node_id"] and pod_info["node_id"] in nodes:
                node_name = nodes[pod_info["node_id"]]["node_name"]
                
            pod_list.append({
                "pod_id": pod_id,
                "cpu_request": pod_info["cpu_request"],
                "node_id": pod_info["node_id"],
                "node_name": node_name,
                "status": pod_info["status"],
                "created_at": pod_info["created_at"]
            })
            
        return jsonify({"pods": pod_list}), 200
        
    except Exception as e:
        logger.error(f"Error listing pods: {str(e)}")
        return jsonify({"error": "Failed to list pods", "details": str(e)}), 500


@app.route("/pods/delete/<pod_id>", methods=["DELETE"])
def delete_pod(pod_id):
    """
    Deletes a pod and releases its resources back to the node.
    """
    try:
        if pod_id not in pods:
            return jsonify({"error": f"Pod {pod_id} not found"}), 404
            
        pod_info = pods[pod_id]
        node_id = pod_info["node_id"]
        
        # Free up resources on the node if pod was assigned
        if node_id and node_id in nodes:
            node = nodes[node_id]
            if pod_id in node["pods"]:
                node["pods"].remove(pod_id)
                node["available_cpu"] += pod_info["cpu_request"]
        
        # Remove pod
        del pods[pod_id]
        
        # Log important event: Pod deleted
        logger.info(f"Pod deleted: {pod_id}")
        
        return jsonify({"message": f"Pod {pod_id} deleted successfully"}), 200
        
    except Exception as e:
        logger.error(f"Error deleting pod {pod_id}: {str(e)}")
        return jsonify({"error": f"Failed to delete pod {pod_id}", "details": str(e)}), 500


@app.route("/nodes/refresh", methods=["POST"])
def refresh_nodes():
    """
    Force a refresh of all node statuses by checking their container states.
    """
    try:
        active_containers = {c.id: c for c in client.containers.list(all=True)}
        nodes_updated = []
        
        for node_id, node_info in nodes.items():
            container_id = node_info["container_id"]
            old_status = node_info["status"]
            
            # Check if container still exists and its status
            if container_id in active_containers:
                container = active_containers[container_id]
                if container.status == "running":
                    node_info["status"] = "Ready"
                else:
                    node_info["status"] = "NotReady"
            else:
                node_info["status"] = "NotReady"
                
            # If status changed, handle it
            if old_status != node_info["status"]:
                if old_status == "Ready" and node_info["status"] == "NotReady":
                    handle_node_failure(node_id)
                
                nodes_updated.append({
                    "node_id": node_id,
                    "node_name": node_info["node_name"],
                    "old_status": old_status,
                    "new_status": node_info["status"]
                })
                
        return jsonify({
            "message": "Node statuses refreshed successfully",
            "nodes_updated": nodes_updated
        }), 200
        
    except Exception as e:
        logger.error(f"Error refreshing node statuses: {str(e)}")
        return jsonify({"error": "Failed to refresh node statuses", "details": str(e)}), 500


@app.route("/health", methods=["GET"])
def health_check():
    """Simple health check endpoint"""
    return jsonify({
        "status": "healthy", 
        "node_count": len(nodes),
        "pod_count": len(pods),
        "last_health_check": time.strftime('%Y-%m-%d %H:%M:%S')
    }), 200


def best_fit_scheduler(cpu_request: float) -> Optional[str]:
    """
    Finds the best node to schedule a pod using Best-Fit algorithm.
    """
    best_node_id = None
    smallest_sufficient_capacity = float('inf')
    
    for node_id, node_info in nodes.items():
        # Skip nodes that aren't Ready
        if node_info["status"] != "Ready":
            continue
            
        available_cpu = node_info["available_cpu"]
        
        # Check if node has enough available CPU
        if available_cpu >= cpu_request:
            # Find the node with smallest sufficient capacity (best fit)
            remaining_capacity = available_cpu - cpu_request
            if remaining_capacity < smallest_sufficient_capacity:
                smallest_sufficient_capacity = remaining_capacity
                best_node_id = node_id
    
    return best_node_id


def handle_node_failure(node_id):
    """
    Handle the failure of a node by rescheduling its pods to other nodes.
    """
    if node_id not in nodes:
        return
        
    failed_node = nodes[node_id]
    pod_ids = failed_node["pods"].copy()  # Copy to avoid modification during iteration
    
    if not pod_ids:
        return
        
    # Log important event: Node failure
    logger.info(f"Node failure detected: {failed_node['node_name']}")
    
    for pod_id in pod_ids:
        if pod_id not in pods:
            # Remove invalid pod reference
            failed_node["pods"].remove(pod_id)
            continue
            
        pod_info = pods[pod_id]
        cpu_request = pod_info["cpu_request"]
        
        # Find a new node using Best-Fit algorithm
        new_node_id = best_fit_scheduler(cpu_request)
        
        if new_node_id:
            # Move pod to new node
            failed_node["pods"].remove(pod_id)
            nodes[new_node_id]["pods"].append(pod_id)
            nodes[new_node_id]["available_cpu"] -= cpu_request
            
            # Update pod information
            pod_info["node_id"] = new_node_id
            pod_info["status"] = "Running"
            
            # Log important event: Pod rescheduled after node failure
            logger.info(f"Pod {pod_id} rescheduled to {nodes[new_node_id]['node_name']} after node failure")
        else:
            # Mark pod as Pending if no suitable node found
            pod_info["status"] = "Pending"
            pod_info["node_id"] = None
            failed_node["pods"].remove(pod_id)


def initialize_app():
    """Initialize the application by checking for existing containers and setting node counter"""
    global current_node_number
    
    try:
        # Find existing containers with our naming pattern
        existing_containers = client.containers.list(all=True, filters={"name": CONTAINER_NAME_PREFIX})
        
        highest_number = 0
        for container in existing_containers:
            # Extract number from container name
            name = container.name
            if name.startswith(CONTAINER_NAME_PREFIX):
                try:
                    number = int(name[len(CONTAINER_NAME_PREFIX):])
                    highest_number = max(highest_number, number)
                except ValueError:
                    # Not a number, skip
                    pass
        
        # Set current counter to the highest found number
        current_node_number = highest_number
        
        # Log important event: Simulator started
        logger.info("Kubernetes Cluster Simulator started")
        
    except Exception as e:
        logger.error(f"Error during initialization: {str(e)}")


if __name__ == "__main__":
    initialize_app()
    app.run(host="0.0.0.0", port=5000, debug=False)