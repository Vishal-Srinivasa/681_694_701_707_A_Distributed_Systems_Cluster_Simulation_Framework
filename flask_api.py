from flask import Flask, request, jsonify
import docker
import uuid
import logging
from typing import Dict, Any


app = Flask(__name__)
client = docker.from_env()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


CONTAINER_BASE_IMAGE = "ubuntu:latest"  # Base image for the containers
CONTAINER_NAME_PREFIX = "node-"
CPU_UNITS_PER_CORE = 100000

# Store registered nodes
nodes: Dict[str, Dict[str, Any]] = {}

# Track the current highest node number
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
    
    Request:
        JSON payload with "cpu_cores" (number): CPU cores to allocate
        
    Response:
        JSON with node_id, node_name, and cpu_cores information
    """
    try:
        # Parse and validate request data
        data = request.json
        if not data:
            return jsonify({"error": "Missing JSON payload"}), 400
            
        cpu_cores = data.get("cpu_cores")
        if not cpu_cores or not isinstance(cpu_cores, (int, float)) or cpu_cores <= 0:
            return jsonify({"error": "Invalid or missing 'cpu_cores'. Must be a positive number."}), 404

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
            "container_id": container.id
        }

        logger.info(f"Node {node_id} added with {cpu_cores} CPU cores. Container name: {node_name}")
        
        # Return success response with node details
        return jsonify({
            "message": "Node added successfully",
            "node_id": node_id,
            "node_name": node_name,
            "cpu_cores": cpu_cores
        }), 200

    except Exception as e:
        logger.error(f"Error adding node: {str(e)}", exc_info=True)
        return jsonify({"error": "Failed to add node", "details": str(e)}), 500


@app.route("/nodes/list", methods=["GET"])
def list_nodes():
    """
    Returns a list of all active nodes and their details.
    
    Response:
        JSON with array of nodes containing node_id, node_name, cpu_cores, and container_id
    """
    try:
        # Refresh container status from Docker engine
        active_containers = {c.id: c for c in client.containers.list(all=True)}
        
        # Build response with detailed node information
        node_list = []
        for node_id, node_info in nodes.items():
            container_id = node_info["container_id"]
            
            # Check if container still exists
            if container_id in active_containers:
                container = active_containers[container_id]
                status = container.status
            else:
                status = "unknown"
                
            node_list.append({
                "node_id": node_id,
                "node_name": node_info["node_name"],
                "cpu_cores": node_info["cpu_cores"],
                "container_id": container_id,
                "status": status
            })
            
        return jsonify({"nodes": node_list}), 200
        
    except Exception as e:
        logger.error(f"Error listing nodes: {str(e)}", exc_info=True)
        return jsonify({"error": "Failed to list nodes", "details": str(e)}), 500


@app.route("/nodes/remove/<node_id>", methods=["DELETE"])
def remove_node(node_id):
    """
    Removes a node and stops its associated container.
    
    Parameters:
        node_id (str): The ID of the node to remove
        
    Response:
        JSON with success/error message
    """
    try:
        if node_id not in nodes:
            return jsonify({"error": f"Node {node_id} not found"}), 404
            
        # Get container ID and remove container
        container_id = nodes[node_id]["container_id"]
        container = client.containers.get(container_id)
        container.stop()
        container.remove()
        
        # Remove node from storage
        node_info = nodes.pop(node_id)
        logger.info(f"Node {node_id} ({node_info['node_name']}) removed successfully")
        
        return jsonify({"message": f"Node {node_id} removed successfully"}), 200
        
    except Exception as e:
        logger.error(f"Error removing node {node_id}: {str(e)}", exc_info=True)
        return jsonify({"error": f"Failed to remove node {node_id}", "details": str(e)}), 500


@app.route("/health", methods=["GET"])
def health_check():
    """Simple health check endpoint"""
    return jsonify({"status": "healthy", "node_count": len(nodes)}), 200


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
        logger.info(f"Initialized with highest node number: {current_node_number}")
        
    except Exception as e:
        logger.error(f"Error during initialization: {str(e)}", exc_info=True)


if __name__ == "__main__":
    logger.info("Starting Docker Node Manager API")
    initialize_app()
    app.run(host="0.0.0.0", port=5000, debug=True)