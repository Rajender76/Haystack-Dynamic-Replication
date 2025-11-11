# cache_service.py
"""
Haystack Cache Service
"""

from flask import Flask, request, jsonify
import hashlib
import time
import threading
import logging
from collections import OrderedDict
import requests
import json
import bisect
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LRUCache:
    """
    Thread-safe LRU (Least Recently Used) cache implementation.
    Evicts least recently used items when capacity is reached.
    """
    def __init__(self, capacity_mb=1000):
        """
        Args:
            capacity_mb: Maximum cache size in megabytes
        """
        self.capacity_bytes = capacity_mb * 1024 * 1024
        self.current_size = 0
        self.cache = OrderedDict()  # Maintains insertion order
        self.lock = threading.Lock()
        
        # Statistics
        self.hits = 0
        self.misses = 0
        self.evictions = 0
    
    def get(self, key):
        """Get value from cache, returns None if not found"""
        with self.lock:
            if key in self.cache:
                # Move to end (most recently used)
                self.cache.move_to_end(key)
                self.hits += 1
                return self.cache[key]
            else:
                self.misses += 1
                return None
    
    def put(self, key, value):
        """Put value in cache, evict LRU if necessary"""
        with self.lock:
            value_size = len(value) if isinstance(value, bytes) else len(str(value))
            
            # If key exists, remove old entry first
            if key in self.cache:
                old_value = self.cache[key]
                old_size = len(old_value) if isinstance(old_value, bytes) else len(str(old_value))
                self.current_size -= old_size
                del self.cache[key]
            
            # Evict until we have space
            while self.current_size + value_size > self.capacity_bytes and self.cache:
                evicted_key, evicted_value = self.cache.popitem(last=False)
                evicted_size = len(evicted_value) if isinstance(evicted_value, bytes) else len(str(evicted_value))
                self.current_size -= evicted_size
                self.evictions += 1
                logger.debug(f"Evicted {evicted_key} ({evicted_size} bytes)")
            
            # Add new entry
            if self.current_size + value_size <= self.capacity_bytes:
                self.cache[key] = value
                self.current_size += value_size
                return True
            else:
                logger.warning(f"Cannot cache {key}: value too large ({value_size} bytes)")
                return False
    
    def delete(self, key):
        """Delete key from cache"""
        with self.lock:
            if key in self.cache:
                value = self.cache[key]
                value_size = len(value) if isinstance(value, bytes) else len(str(value))
                self.current_size -= value_size
                del self.cache[key]
                return True
            return False
    
    def get_stats(self):
        """Get cache statistics"""
        with self.lock:
            total_requests = self.hits + self.misses
            hit_rate = (self.hits / total_requests * 100) if total_requests > 0 else 0
            
            return {
                'hits': self.hits,
                'misses': self.misses,
                'hit_rate': f"{hit_rate:.2f}%",
                'evictions': self.evictions,
                'current_size_mb': self.current_size / (1024 * 1024),
                'capacity_mb': self.capacity_bytes / (1024 * 1024),
                'utilization': f"{(self.current_size / self.capacity_bytes * 100):.2f}%",
                'item_count': len(self.cache)
            }
    
    def clear(self):
        """Clear entire cache"""
        with self.lock:
            self.cache.clear()
            self.current_size = 0


class ConsistentHashRing:
    """Consistent hashing for distributing cache keys across nodes"""
    def __init__(self, nodes=None, virtual_nodes=150):
        self.virtual_nodes = virtual_nodes
        self.ring = []
        self.nodes = set()
        
        if nodes:
            for node in nodes:
                self.add_node(node)
    
    def _hash(self, key):
        """Generate hash value for a key"""
        return int(hashlib.md5(str(key).encode()).hexdigest(), 16)
    
    def add_node(self, node_id):
        """Add a node with virtual nodes"""
        if node_id in self.nodes:
            return
        
        self.nodes.add(node_id)
        
        for i in range(self.virtual_nodes):
            virtual_key = f"{node_id}:vnode:{i}"
            hash_value = self._hash(virtual_key)
            bisect.insort(self.ring, (hash_value, node_id))
        
        logger.info(f"Added cache node {node_id} to hash ring")
    
    def remove_node(self, node_id):
        """Remove a node from the ring"""
        if node_id not in self.nodes:
            return
        
        self.nodes.remove(node_id)
        self.ring = [(h, n) for h, n in self.ring if n != node_id]
        logger.info(f"Removed cache node {node_id} from hash ring")
    
    def get_node(self, key):
        """Get the node responsible for a key"""
        if not self.ring:
            return None
        
        hash_value = self._hash(key)
        idx = bisect.bisect_right(self.ring, (hash_value, ''))
        
        if idx == len(self.ring):
            idx = 0
        
        return self.ring[idx][1]
    
    def get_nodes(self, key, count=2):
        """Get multiple nodes for replication"""
        if not self.ring:
            return []
        
        hash_value = self._hash(key)
        idx = bisect.bisect_right(self.ring, (hash_value, ''))
        
        if idx == len(self.ring):
            idx = 0
        
        selected_nodes = []
        visited = set()
        
        while len(selected_nodes) < count and len(visited) < len(self.ring):
            node_id = self.ring[idx][1]
            if node_id not in selected_nodes:
                selected_nodes.append(node_id)
            
            visited.add(idx)
            idx = (idx + 1) % len(self.ring)
        
        return selected_nodes


class GossipProtocol:
    """
    Simple gossip protocol for node membership and failure detection.
    Nodes periodically exchange heartbeats with peers.
    """
    def __init__(self, self_address, peer_addresses, heartbeat_interval=5):
        self.self_address = self_address
        self.peer_addresses = peer_addresses
        self.heartbeat_interval = heartbeat_interval
        self.peer_status = {peer: {'alive': True, 'last_seen': time.time()} 
                           for peer in peer_addresses}
        self.lock = threading.Lock()
        
        # Start gossip thread
        threading.Thread(target=self._gossip_loop, daemon=True).start()
        threading.Thread(target=self._failure_detector, daemon=True).start()
    
    def _gossip_loop(self):
        """Periodically send heartbeats to peers"""
        while True:
            try:
                time.sleep(self.heartbeat_interval)
                
                for peer in self.peer_addresses:
                    try:
                        response = requests.post(
                            f"http://{peer}/gossip/heartbeat",
                            json={'from': self.self_address, 'timestamp': time.time()},
                            timeout=2
                        )
                        
                        if response.status_code == 200:
                            with self.lock:
                                self.peer_status[peer]['alive'] = True
                                self.peer_status[peer]['last_seen'] = time.time()
                    
                    except Exception as e:
                        logger.debug(f"Heartbeat to {peer} failed: {e}")
            
            except Exception as e:
                logger.error(f"Gossip loop error: {e}")
    
    def _failure_detector(self):
        """Detect failed nodes based on missed heartbeats"""
        while True:
            try:
                time.sleep(self.heartbeat_interval * 2)
                timeout = self.heartbeat_interval * 3
                
                with self.lock:
                    current_time = time.time()
                    for peer, status in self.peer_status.items():
                        if status['alive'] and (current_time - status['last_seen']) > timeout:
                            status['alive'] = False
                            logger.warning(f"Detected peer failure: {peer}")
            
            except Exception as e:
                logger.error(f"Failure detector error: {e}")
    
    def get_alive_peers(self):
        """Get list of currently alive peers"""
        with self.lock:
            return [peer for peer, status in self.peer_status.items() if status['alive']]
    
    def receive_heartbeat(self, from_address):
        """Process received heartbeat"""
        with self.lock:
            if from_address in self.peer_status:
                self.peer_status[from_address]['alive'] = True
                self.peer_status[from_address]['last_seen'] = time.time()


class CacheServer:
    """
    Main Cache service handling photo caching with distributed features.
    """
    
    def __init__(self, address, peer_addresses, capacity_mb=1000, 
                 directory_nodes=None, replication_factor=2):
        """
        Args:
            address: This cache node's address (host:port)
            peer_addresses: List of other cache node addresses
            capacity_mb: Cache capacity in megabytes
            directory_nodes: List of Directory service addresses
            replication_factor: Number of cache replicas for each photo
        """
        self.address = address
        self.peer_addresses = peer_addresses
        self.directory_nodes = directory_nodes or []
        self.replication_factor = replication_factor
        
        # Initialize cache
        self.cache = LRUCache(capacity_mb=capacity_mb)
        
        # Consistent hash ring for determining ownership
        all_nodes = [address] + peer_addresses
        self.hash_ring = ConsistentHashRing(nodes=all_nodes)
        
        # Gossip protocol for failure detection
        self.gossip = GossipProtocol(address, peer_addresses)
        
        # Flask app
        self.app = Flask(__name__)
        self.setup_routes()
        
        # Track which volumes are write-enabled (for selective caching)
        self.write_enabled_volumes = set()
        self.volume_lock = threading.Lock()
        
        logger.info(f"Cache server initialized: {address}, capacity: {capacity_mb}MB")
    
    def setup_routes(self):
        """Setup Flask HTTP routes"""
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            """Health check endpoint"""
            return jsonify({
                'status': 'healthy',
                'address': self.address,
                'cache_stats': self.cache.get_stats()
            })
        
        @self.app.route('/get', methods=['GET'])
        def get_photo():
            """Get photo from cache"""
            photo_id = request.args.get('photo_id')
            
            if not photo_id:
                return jsonify({'error': 'photo_id required'}), 400
            
            # Check if this node should handle this key
            responsible_node = self.hash_ring.get_node(photo_id)
            
            if responsible_node != self.address:
                # Forward to responsible node
                try:
                    response = requests.get(
                        f"http://{responsible_node}/get_local",
                        params={'photo_id': photo_id},
                        timeout=2
                    )
                    return response.json(), response.status_code
                except Exception as e:
                    logger.error(f"Forward to {responsible_node} failed: {e}")
                    return jsonify({'error': 'Cache miss'}), 404
            
            # This node is responsible
            data = self.cache.get(photo_id)
            
            if data:
                return jsonify({
                    'photo_id': photo_id,
                    'data': data.hex() if isinstance(data, bytes) else data,
                    'source': 'cache',
                    'node': self.address
                })
            
            return jsonify({'error': 'Cache miss'}), 404
        
        @self.app.route('/get_local', methods=['GET'])
        def get_photo_local():
            """Get photo from local cache only (used by forwarding)"""
            photo_id = request.args.get('photo_id')
            data = self.cache.get(photo_id)
            
            if data:
                return jsonify({
                    'photo_id': photo_id,
                    'data': data.hex() if isinstance(data, bytes) else data,
                    'source': 'cache',
                    'node': self.address
                })
            
            return jsonify({'error': 'Cache miss'}), 404
        
        @self.app.route('/put', methods=['POST'])
        def put_photo():
            """
            Put photo in cache (called by Store after successful write).
            Only caches photos from write-enabled volumes.
            """
            data = request.json
            photo_id = data.get('photo_id')
            photo_data = data.get('data')
            volume_id = data.get('volume_id')
            
            if not all([photo_id, photo_data]):
                return jsonify({'error': 'photo_id and data required'}), 400
            
            # Selective caching: only cache from write-enabled volumes
            if volume_id and not self._should_cache_volume(volume_id):
                logger.debug(f"Skipping cache for read-only volume: {volume_id}")
                return jsonify({'status': 'skipped', 'reason': 'read-only volume'})
            
            # Convert hex string back to bytes
            if isinstance(photo_data, str):
                photo_data = bytes.fromhex(photo_data)
            
            # Check if this node should handle this key
            responsible_nodes = self.hash_ring.get_nodes(photo_id, self.replication_factor)
            
            if self.address in responsible_nodes:
                # Cache locally
                success = self.cache.put(photo_id, photo_data)
                
                # Replicate to other responsible nodes asynchronously
                for node in responsible_nodes:
                    if node != self.address:
                        threading.Thread(
                            target=self._replicate_to_peer,
                            args=(node, photo_id, photo_data)
                        ).start()
                
                return jsonify({
                    'status': 'cached',
                    'node': self.address,
                    'replicated_to': [n for n in responsible_nodes if n != self.address]
                })
            else:
                # Forward to responsible node
                try:
                    response = requests.post(
                        f"<http://{responsible_nodes>[0]}/put",
                        json=data,
                        timeout=2
                    )
                    return response.json(), response.status_code
                except Exception as e:
                    logger.error(f"Forward put to {responsible_nodes[0]} failed: {e}")
                    return jsonify({'error': 'Forward failed'}), 500
        
        @self.app.route('/put_local', methods=['POST'])
        def put_photo_local():
            """Put photo in local cache only (used by replication)"""
            data = request.json
            photo_id = data.get('photo_id')
            photo_data = data.get('data')
            
            if isinstance(photo_data, str):
                photo_data = bytes.fromhex(photo_data)
            
            success = self.cache.put(photo_id, photo_data)
            return jsonify({'status': 'success' if success else 'failed'})
        
        @self.app.route('/delete', methods=['POST'])
        def delete_photo():
            """Delete photo from cache"""
            data = request.json
            photo_id = data.get('photo_id')
            
            success = self.cache.delete(photo_id)
            
            # Also delete from replicas
            responsible_nodes = self.hash_ring.get_nodes(photo_id, self.replication_factor)
            for node in responsible_nodes:
                if node != self.address:
                    try:
                        requests.post(
                            f"http://{node}/delete_local",
                            json={'photo_id': photo_id},
                            timeout=1
                        )
                    except:
                        pass
            
            return jsonify({'status': 'success' if success else 'not_found'})
        
        @self.app.route('/delete_local', methods=['POST'])
        def delete_photo_local():
            """Delete photo from local cache only"""
            data = request.json
            photo_id = data.get('photo_id')
            success = self.cache.delete(photo_id)
            return jsonify({'status': 'success' if success else 'not_found'})
        
        @self.app.route('/stats', methods=['GET'])
        def get_stats():
            """Get cache statistics"""
            stats = self.cache.get_stats()
            stats['address'] = self.address
            stats['alive_peers'] = len(self.gossip.get_alive_peers())
            stats['total_peers'] = len(self.peer_addresses)
            return jsonify(stats)
        
        @self.app.route('/gossip/heartbeat', methods=['POST'])
        def receive_heartbeat():
            """Receive heartbeat from peer cache node"""
            data = request.json
            from_address = data.get('from')
            self.gossip.receive_heartbeat(from_address)
            return jsonify({'status': 'success'})
        
        @self.app.route('/clear', methods=['POST'])
        def clear_cache():
            """Clear entire cache (for testing)"""
            self.cache.clear()
            return jsonify({'status': 'cleared'})
    
    def _should_cache_volume(self, volume_id):
        """
        Determine if photos from this volume should be cached.
        Only cache write-enabled volumes (newly uploaded photos get immediate traffic).
        """
        # For simplicity, cache all volumes for now
        # In production, query Directory for volume status
        return True
    
    def _replicate_to_peer(self, peer_address, photo_id, photo_data):
        """Replicate cached photo to peer node"""
        try:
            requests.post(
                f"http://{peer_address}/put_local",
                json={
                    'photo_id': photo_id,
                    'data': photo_data.hex() if isinstance(photo_data, bytes) else photo_data
                },
                timeout=2
            )
            logger.debug(f"Replicated {photo_id} to {peer_address}")
        except Exception as e:
            logger.warning(f"Replication to {peer_address} failed: {e}")
    
    def run(self, port):
        """Start the Flask HTTP server"""
        self.app.run(host='0.0.0.0', port=port, threaded=True)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Haystack Cache Service')
    parser.add_argument('--address', required=True, help='This node address (host:port)')
    parser.add_argument('--peers', nargs='+', default=[], help='Peer cache addresses')
    parser.add_argument('--port', type=int, required=True, help='HTTP port')
    parser.add_argument('--capacity', type=int, default=1000, help='Cache capacity in MB')
    parser.add_argument('--directory-nodes', nargs='+', default=[], help='Directory addresses')
    
    args = parser.parse_args()
    
    server = CacheServer(
        address=args.address,
        peer_addresses=args.peers,
        capacity_mb=args.capacity,
        directory_nodes=args.directory_nodes,
        replication_factor=2
    )
    
    logger.info(f"Starting Cache server on port {args.port}")
    server.run(args.port)


if __name__ == '__main__':
    main()
