# directory_service.py
"""
Haystack Directory Service
"""

from flask import Flask, request, jsonify, redirect
from pysyncobj import SyncObj, SyncObjConf, replicated
from pysyncobj.batteries import ReplDict, ReplList
import hashlib
import time
import threading
import logging
import json
import sys
from collections import defaultdict
import bisect

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ConsistentHashRing:
    """
    Consistent hashing implementation for distributing photos across volumes.
    Uses virtual nodes for better load distribution.
    """
    def __init__(self, virtual_nodes=150):
        self.virtual_nodes = virtual_nodes
        self.ring = []  # Sorted list of (hash_value, node_id) tuples
        self.nodes = set()
    
    def _hash(self, key):
        """Generate hash value for a key"""
        return int(hashlib.md5(str(key).encode()).hexdigest(), 16)
    
    def add_node(self, node_id):
        """Add a node to the hash ring with virtual nodes"""
        if node_id in self.nodes:
            return
        
        self.nodes.add(node_id)
        
        # Add virtual nodes
        for i in range(self.virtual_nodes):
            virtual_key = f"{node_id}:vnode:{i}"
            hash_value = self._hash(virtual_key)
            bisect.insort(self.ring, (hash_value, node_id))
        
        logger.info(f"Added node {node_id} to hash ring with {self.virtual_nodes} virtual nodes")
    
    def remove_node(self, node_id):
        """Remove a node from the hash ring"""
        if node_id not in self.nodes:
            return
        
        self.nodes.remove(node_id)
        self.ring = [(h, n) for h, n in self.ring if n != node_id]
        logger.info(f"Removed node {node_id} from hash ring")
    
    def get_node(self, key):
        """Get the node responsible for a key"""
        if not self.ring:
            return None
        
        hash_value = self._hash(key)
        
        # Binary search for the first node with hash >= key's hash
        idx = bisect.bisect_right(self.ring, (hash_value, ''))
        
        if idx == len(self.ring):
            idx = 0
        
        return self.ring[idx][1]
    
    def get_nodes(self, key, count=3):
        """Get multiple nodes for a key (for replication)"""
        if not self.ring or count > len(self.nodes):
            return list(self.nodes)[:count] if self.nodes else []
        
        hash_value = self._hash(key)
        idx = bisect.bisect_right(self.ring, (hash_value, ''))
        
        if idx == len(self.ring):
            idx = 0
        
        # Collect unique nodes
        selected_nodes = []
        visited_indices = set()
        
        while len(selected_nodes) < count and len(visited_indices) < len(self.ring):
            node_id = self.ring[idx][1]
            if node_id not in selected_nodes:
                selected_nodes.append(node_id)
            
            visited_indices.add(idx)
            idx = (idx + 1) % len(self.ring)
        
        return selected_nodes


class DirectoryStateMachine(SyncObj):
    """
    Raft-based state machine for Directory metadata.
    All state changes go through Raft consensus.
    """
    def __init__(self, self_address, partner_addresses):
        cfg = SyncObjConf(
            fullDumpFile=f'directory_dump_{self_address.replace(":", "_")}.bin',
            logCompactionMinEntries=100,
            dynamicMembershipChange=True,
            appendEntriesUseBatch=True
        )
        super(DirectoryStateMachine, self).__init__(self_address, partner_addresses, cfg)
        
        # Replicated state across Raft cluster
        self.volume_mappings = ReplDict()  # volume_id -> [store_machine_ids]
        self.photo_locations = ReplDict()  # photo_id -> {volume_id, replicas, replication_factor}
        self.store_machines = ReplDict()  # store_id -> {address, status, last_heartbeat, capacity}
        self.volume_status = ReplDict()   # volume_id -> {status, capacity_used, capacity_total}
        self.volume_counter = ReplDict()  # counter for generating volume IDs
        
        # Initialize counter if not exists
        if 'next_volume_id' not in self.volume_counter:
            self.volume_counter['next_volume_id'] = 1
    
    @replicated
    def register_store_machine(self, store_id, address, capacity_gb=100):
        """Register a new store machine (replicated via Raft)"""
        self.store_machines[store_id] = {
            'address': address,
            'status': 'active',
            'last_heartbeat': time.time(),
            'capacity_gb': capacity_gb,
            'used_gb': 0
        }
        logger.info(f"Registered store machine: {store_id} at {address}")
        return True
    
    @replicated
    def update_heartbeat(self, store_id):
        """Update heartbeat timestamp for store machine"""
        if store_id in self.store_machines:
            store_info = dict(self.store_machines[store_id])
            store_info['last_heartbeat'] = time.time()
            store_info['status'] = 'active'
            self.store_machines[store_id] = store_info
            return True
        return False
    
    @replicated
    def mark_store_failed(self, store_id):
        """Mark a store machine as failed"""
        if store_id in self.store_machines:
            store_info = dict(self.store_machines[store_id])
            store_info['status'] = 'failed'
            self.store_machines[store_id] = store_info
            logger.warning(f"Marked store {store_id} as failed")
            return True
        return False
    
    @replicated
    def create_logical_volume(self, store_ids, capacity_gb=100):
        """Create new logical volume across store machines"""
        volume_id = f"vol_{self.volume_counter['next_volume_id']}"
        
        # Increment counter
        counter = dict(self.volume_counter)
        counter['next_volume_id'] = counter['next_volume_id'] + 1
        self.volume_counter.update(counter)
        
        # Create volume mapping
        self.volume_mappings[volume_id] = store_ids
        
        # Initialize volume status
        self.volume_status[volume_id] = {
            'status': 'writable',
            'capacity_used': 0,
            'capacity_total': capacity_gb,
            'photo_count': 0
        }
        
        logger.info(f"Created volume {volume_id} on stores: {store_ids}")
        return volume_id
    
    @replicated
    def mark_volume_readonly(self, volume_id):
        """Mark a volume as read-only (full)"""
        if volume_id in self.volume_status:
            vol_status = dict(self.volume_status[volume_id])
            vol_status['status'] = 'readonly'
            self.volume_status[volume_id] = vol_status
            logger.info(f"Marked volume {volume_id} as readonly")
            return True
        return False
    
    @replicated
    def assign_photo_to_volume(self, photo_id, volume_id, replica_stores, size_mb=0):
        """Assign photo to volume and stores (consensus operation)"""
        self.photo_locations[photo_id] = {
            'volume_id': volume_id,
            'replicas': replica_stores,
            'replication_factor': len(replica_stores),
            'size_mb': size_mb,
            'timestamp': time.time()
        }
        
        # Update volume capacity
        if volume_id in self.volume_status:
            vol_status = dict(self.volume_status[volume_id])
            vol_status['capacity_used'] += size_mb
            vol_status['photo_count'] += 1
            self.volume_status[volume_id] = vol_status
        
        return True
    
    @replicated
    def add_replica_location(self, photo_id, new_store_id):
        """Add new replica location for dynamic replication"""
        if photo_id in self.photo_locations:
            location = dict(self.photo_locations[photo_id])
            if new_store_id not in location['replicas']:
                location['replicas'].append(new_store_id)
                location['replication_factor'] = len(location['replicas'])
                self.photo_locations[photo_id] = location
                logger.info(f"Added replica for {photo_id} on {new_store_id}")
            return True
        return False
    
    @replicated
    def remove_replica_location(self, photo_id, store_id):
        """Remove replica location (for cooling down hot photos)"""
        if photo_id in self.photo_locations:
            location = dict(self.photo_locations[photo_id])
            if store_id in location['replicas'] and len(location['replicas']) > 3:
                location['replicas'].remove(store_id)
                location['replication_factor'] = len(location['replicas'])
                self.photo_locations[photo_id] = location
                logger.info(f"Removed replica for {photo_id} from {store_id}")
            return True
        return False


class DirectoryServer:
    """
    Main Directory service handling client requests and cluster coordination.
    """
    
    def __init__(self, self_address, partner_addresses, http_port):
        """
        Args:
            self_address: Raft address for this node (e.g., 'localhost:4321')
            partner_addresses: List of partner Raft addresses
            http_port: HTTP port for client requests
        """
        self.self_address = self_address
        self.http_port = http_port
        
        # Initialize Raft state machine
        self.state_machine = DirectoryStateMachine(self_address, partner_addresses)
        
        # Consistent hash ring for photo distribution
        self.hash_ring = ConsistentHashRing(virtual_nodes=150)
        
        # Flask app for HTTP API
        self.app = Flask(__name__)
        self.setup_routes()
        
        # Start background threads
        threading.Thread(target=self.heartbeat_monitor, daemon=True).start()
        threading.Thread(target=self.volume_manager, daemon=True).start()
        
        logger.info(f"Directory server initialized: HTTP port {http_port}, Raft {self_address}")
    
    def setup_routes(self):
        """Setup Flask HTTP routes"""
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            """Health check endpoint"""
            return jsonify({
                'status': 'healthy',
                'is_leader': self.state_machine._isLeader(),
                'leader': self.state_machine._getLeader(),
                'raft_address': self.self_address
            })
        
        @self.app.route('/register_store', methods=['POST'])
        def register_store():
            """Register a new store machine - LEADER ONLY"""
            if not self.state_machine._isLeader():
                return self._redirect_to_leader()
            
            data = request.json
            store_id = data['store_id']
            address = data['address']
            capacity_gb = data.get('capacity_gb', 100)
            
            success = self.state_machine.register_store_machine(store_id, address, capacity_gb)
            
            if success:
                # Add to consistent hash ring
                self.hash_ring.add_node(store_id)
                
                return jsonify({
                    'status': 'success',
                    'store_id': store_id
                })
            return jsonify({'error': 'Registration failed'}), 500
        
        @self.app.route('/heartbeat', methods=['POST'])
        def heartbeat():
            """Receive heartbeat from store machine"""
            data = request.json
            store_id = data['store_id']
            
            success = self.state_machine.update_heartbeat(store_id)
            
            return jsonify({'status': 'success' if success else 'failed'})
        
        @self.app.route('/assign_volume', methods=['POST'])
        def assign_volume():
            """Assign volume for photo upload - LEADER ONLY"""
            if not self.state_machine._isLeader():
                return self._redirect_to_leader()
            
            data = request.json
            photo_id = data['photo_id']
            size_mb = data.get('size_mb', 1)
            
            # Use consistent hashing to select volume
            volume_id = self._select_volume_for_photo(photo_id)
            
            # Get replica stores for this volume
            if volume_id and volume_id in self.state_machine.volume_mappings:
                replica_stores = list(self.state_machine.volume_mappings[volume_id])
            else:
                # Create new volume if needed
                replica_stores = self.hash_ring.get_nodes(photo_id, count=3)
                if replica_stores:
                    volume_id = self.state_machine.create_logical_volume(replica_stores)
                else:
                    return jsonify({'error': 'No stores available'}), 503
            
            # Assign photo to volume via Raft consensus
            success = self.state_machine.assign_photo_to_volume(
                photo_id, volume_id, replica_stores, size_mb
            )
            
            if success:
                # Get store addresses
                store_addresses = {}
                for store_id in replica_stores:
                    store_info = self.state_machine.store_machines.get(store_id)
                    if store_info:
                        store_addresses[store_id] = store_info['address']
                
                return jsonify({
                    'photo_id': photo_id,
                    'volume_id': volume_id,
                    'replicas': replica_stores,
                    'primary': replica_stores[0],
                    'store_addresses': store_addresses
                })
            
            return jsonify({'error': 'Assignment failed'}), 500
        
        @self.app.route('/get_photo_location', methods=['GET'])
        def get_photo_location():
            """Get replica locations for photo - CAN BE SERVED BY FOLLOWER"""
            photo_id = request.args.get('photo_id')
            
            if not photo_id:
                return jsonify({'error': 'photo_id required'}), 400
            
            locations = self.state_machine.photo_locations.get(photo_id)
            
            if locations:
                # Build replica list with addresses
                replica_info = []
                for store_id in locations['replicas']:
                    store_data = self.state_machine.store_machines.get(store_id)
                    if store_data and store_data['status'] == 'active':
                        replica_info.append({
                            'store_id': store_id,
                            'address': store_data['address']
                        })
                
                return jsonify({
                    'photo_id': photo_id,
                    'volume_id': locations['volume_id'],
                    'replicas': replica_info,
                    'replication_factor': locations['replication_factor']
                })
            
            return jsonify({'error': 'Photo not found'}), 404
        
        @self.app.route('/add_replica', methods=['POST'])
        def add_replica():
            """Add replica for hot photo - LEADER ONLY (called by Replication Manager)"""
            if not self.state_machine._isLeader():
                return self._redirect_to_leader()
            
            data = request.json
            photo_id = data['photo_id']
            new_store_id = data['store_id']
            
            success = self.state_machine.add_replica_location(photo_id, new_store_id)
            
            return jsonify({'status': 'success' if success else 'failed'})
        
        @self.app.route('/remove_replica', methods=['POST'])
        def remove_replica():
            """Remove replica for cooled photo - LEADER ONLY"""
            if not self.state_machine._isLeader():
                return self._redirect_to_leader()
            
            data = request.json
            photo_id = data['photo_id']
            store_id = data['store_id']
            
            success = self.state_machine.remove_replica_location(photo_id, store_id)
            
            return jsonify({'status': 'success' if success else 'failed'})
        
        @self.app.route('/stats', methods=['GET'])
        def get_stats():
            """Get cluster statistics"""
            stats = {
                'total_stores': len(self.state_machine.store_machines),
                'active_stores': sum(1 for s in self.state_machine.store_machines.values() 
                                   if s['status'] == 'active'),
                'total_volumes': len(self.state_machine.volume_mappings),
                'writable_volumes': sum(1 for v in self.state_machine.volume_status.values() 
                                       if v['status'] == 'writable'),
                'total_photos': len(self.state_machine.photo_locations),
                'is_leader': self.state_machine._isLeader(),
                'leader_address': self.state_machine._getLeader()
            }
            return jsonify(stats)
    
    def _redirect_to_leader(self):
        """Redirect request to current Raft leader"""
        leader = self.state_machine._getLeader()
        if leader:
            # Convert Raft address to HTTP address
            # Raft: localhost:4321 -> HTTP: localhost:8001
            leader_http = self._raft_to_http_address(leader)
            return jsonify({
                'error': 'Not leader',
                'leader': leader_http,
                'redirect': True
            }), 307
        return jsonify({'error': 'No leader available'}), 503
    
    def _raft_to_http_address(self, raft_address):
        """Convert Raft address to HTTP address"""
        # Simple conversion - in production, maintain a mapping
        host, port = raft_address.split(':')
        http_port = int(port) + 3680  # Offset for HTTP port
        return f"{host}:{http_port}"
    
    def _select_volume_for_photo(self, photo_id):
        """Select appropriate volume for photo using consistent hashing"""
        # Find writable volumes
        writable_volumes = [
            vol_id for vol_id, status in self.state_machine.volume_status.items()
            if status['status'] == 'writable' and 
               status['capacity_used'] < status['capacity_total'] * 0.9
        ]
        
        if not writable_volumes:
            return None
        
        # Use consistent hashing on photo_id to select volume
        hash_val = int(hashlib.md5(photo_id.encode()).hexdigest(), 16)
        selected_volume = writable_volumes[hash_val % len(writable_volumes)]
        
        return selected_volume
    
    def heartbeat_monitor(self):
        """Background thread to monitor store machine heartbeats"""
        while True:
            try:
                time.sleep(10)  # Check every 10 seconds
                
                if not self.state_machine._isLeader():
                    continue
                
                current_time = time.time()
                timeout = 30  # 30 second timeout
                
                for store_id, store_info in list(self.state_machine.store_machines.items()):
                    if store_info['status'] == 'active':
                        time_since_heartbeat = current_time - store_info['last_heartbeat']
                        
                        if time_since_heartbeat > timeout:
                            logger.warning(f"Store {store_id} missed heartbeat, marking as failed")
                            self.state_machine.mark_store_failed(store_id)
                            self.hash_ring.remove_node(store_id)
            
            except Exception as e:
                logger.error(f"Error in heartbeat monitor: {e}")
    
    def volume_manager(self):
        """Background thread to manage volumes"""
        while True:
            try:
                time.sleep(60)  # Check every minute
                
                if not self.state_machine._isLeader():
                    continue
                
                # Check if we need more writable volumes
                writable_count = sum(
                    1 for v in self.state_machine.volume_status.values()
                    if v['status'] == 'writable'
                )
                
                if writable_count < 2:  # Maintain at least 2 writable volumes
                    active_stores = [
                        store_id for store_id, info in self.state_machine.store_machines.items()
                        if info['status'] == 'active'
                    ]
                    
                    if len(active_stores) >= 3:
                        replica_stores = active_stores[:3]
                        volume_id = self.state_machine.create_logical_volume(replica_stores)
                        logger.info(f"Auto-created new volume: {volume_id}")
            
            except Exception as e:
                logger.error(f"Error in volume manager: {e}")
    
    def run(self):
        """Start the Flask HTTP server"""
        self.app.run(host='0.0.0.0', port=self.http_port, threaded=True)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Haystack Directory Service')
    parser.add_argument('--raft-address', required=True, help='Raft address (e.g., localhost:4321)')
    parser.add_argument('--partners', nargs='+', default=[], help='Partner Raft addresses')
    parser.add_argument('--http-port', type=int, required=True, help='HTTP port')
    
    args = parser.parse_args()
    
    server = DirectoryServer(
        self_address=args.raft_address,
        partner_addresses=args.partners,
        http_port=args.http_port
    )
    
    logger.info(f"Starting Directory server on HTTP port {args.http_port}")
    server.run()


if __name__ == '__main__':
    main()
