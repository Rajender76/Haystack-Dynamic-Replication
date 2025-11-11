# replication_manager.py
"""
Haystack Replication Manager - Dynamic Replication Controller
"""

from flask import Flask, request, jsonify
import threading
import time
import logging
import requests
import hashlib
from collections import defaultdict
from datetime import datetime, timedelta
import json
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HeatScoreCalculator:
    """
    Calculates heat scores for photos based on access patterns.
    Uses time-decay function where recent accesses weigh more.
    """
    def __init__(self, hour_weight=1.0, day_weight=0.3, week_weight=0.1):
        self.hour_weight = hour_weight
        self.day_weight = day_weight
        self.week_weight = week_weight
    
    def calculate_heat_score(self, access_log):
        """
        Calculate heat score for a photo based on access history.
        
        Args:
            access_log: List of access timestamps
        
        Returns:
            float: Heat score (higher = hotter)
        """
        if not access_log:
            return 0.0
        
        current_time = time.time()
        
        # Count accesses in different time windows
        last_hour = current_time - 3600
        last_day = current_time - 86400
        last_week = current_time - 604800
        
        hour_count = sum(1 for t in access_log if t >= last_hour)
        day_count = sum(1 for t in access_log if t >= last_day)
        week_count = sum(1 for t in access_log if t >= last_week)
        
        # Calculate weighted heat score
        heat_score = (
            hour_count * self.hour_weight +
            day_count * self.day_weight +
            week_count * self.week_weight
        )
        
        return heat_score


class AccessTracker:
    """
    Tracks access patterns for all photos with efficient storage.
    Uses sliding window to maintain recent access history.
    """
    def __init__(self, retention_hours=168):  # 7 days default
        self.retention_seconds = retention_hours * 3600
        self.access_logs = defaultdict(list)  # photo_id -> [timestamps]
        self.lock = threading.Lock()
        
        # Start cleanup thread
        threading.Thread(target=self._cleanup_old_entries, daemon=True).start()
    
    def record_access(self, photo_id, timestamp=None):
        """Record an access to a photo"""
        if timestamp is None:
            timestamp = time.time()
        
        with self.lock:
            self.access_logs[photo_id].append(timestamp)
    
    def get_access_log(self, photo_id):
        """Get access log for a photo"""
        with self.lock:
            return self.access_logs.get(photo_id, []).copy()
    
    def get_all_photo_ids(self):
        """Get all tracked photo IDs"""
        with self.lock:
            return list(self.access_logs.keys())
    
    def _cleanup_old_entries(self):
        """Periodically remove old access entries to save memory"""
        while True:
            try:
                time.sleep(3600)  # Run every hour
                
                cutoff_time = time.time() - self.retention_seconds
                
                with self.lock:
                    for photo_id in list(self.access_logs.keys()):
                        # Filter out old timestamps
                        self.access_logs[photo_id] = [
                            t for t in self.access_logs[photo_id]
                            if t >= cutoff_time
                        ]
                        
                        # Remove empty logs
                        if not self.access_logs[photo_id]:
                            del self.access_logs[photo_id]
                
                logger.info(f"Cleanup completed. Tracking {len(self.access_logs)} photos")
            
            except Exception as e:
                logger.error(f"Cleanup error: {e}")


class LeaderElection:
    """
    Simple leader election mechanism.
    In production, use ZooKeeper or etcd for proper distributed consensus.
    """
    def __init__(self, node_id, peer_addresses):
        self.node_id = node_id
        self.peer_addresses = peer_addresses
        self.is_leader = False
        self.leader_id = None
        self.last_heartbeat = time.time()
        self.lock = threading.Lock()
        
        # Start election thread
        threading.Thread(target=self._election_loop, daemon=True).start()
    
    def _election_loop(self):
        """Continuous leader election process"""
        while True:
            try:
                time.sleep(5)  # Check every 5 seconds
                
                if self.is_leader:
                    # Send heartbeats as leader
                    self._send_leader_heartbeat()
                else:
                    # Check if we should start election
                    if time.time() - self.last_heartbeat > 15:
                        logger.info("Leader timeout, starting election")
                        self._start_election()
            
            except Exception as e:
                logger.error(f"Election loop error: {e}")
    
    def _start_election(self):
        """Start leader election"""
        # Simple election: node with lowest ID becomes leader
        all_nodes = [self.node_id] + self.peer_addresses
        
        # Try to contact all peers
        alive_nodes = [self.node_id]
        for peer in self.peer_addresses:
            try:
                response = requests.get(f"http://{peer}/health", timeout=2)
                if response.status_code == 200:
                    alive_nodes.append(peer)
            except:
                pass
        
        # Lowest ID wins
        alive_nodes.sort()
        winner = alive_nodes[0]
        
        with self.lock:
            if winner == self.node_id:
                self.is_leader = True
                self.leader_id = self.node_id
                logger.info(f"🎉 Elected as LEADER: {self.node_id}")
            else:
                self.is_leader = False
                self.leader_id = winner
                logger.info(f"Following leader: {winner}")
    
    def _send_leader_heartbeat(self):
        """Send heartbeat to followers as leader"""
        for peer in self.peer_addresses:
            try:
                requests.post(
                    f"http://{peer}/leader_heartbeat",
                    json={'leader_id': self.node_id, 'timestamp': time.time()},
                    timeout=1
                )
            except:
                pass
    
    def receive_heartbeat(self, leader_id):
        """Receive heartbeat from leader"""
        with self.lock:
            self.leader_id = leader_id
            self.last_heartbeat = time.time()
            if leader_id != self.node_id:
                self.is_leader = False
    
    def get_is_leader(self):
        """Check if this node is the leader"""
        with self.lock:
            return self.is_leader


class ReplicationManager:
    """
    Main Replication Manager service that orchestrates dynamic replication.
    """
    
    def __init__(self, node_id, peer_addresses, directory_nodes,
                 hot_threshold=100, cold_threshold=10, min_replicas=3, max_replicas=7):
        """
        Args:
            node_id: This node's identifier
            peer_addresses: Other replication manager addresses
            directory_nodes: Directory service addresses
            hot_threshold: Access count threshold for hot classification
            cold_threshold: Access count threshold for cold classification
            min_replicas: Minimum replication factor
            max_replicas: Maximum replication factor
        """
        self.node_id = node_id
        self.directory_nodes = directory_nodes
        self.hot_threshold = hot_threshold
        self.cold_threshold = cold_threshold
        self.min_replicas = min_replicas
        self.max_replicas = max_replicas
        
        # Leader election
        self.leader_election = LeaderElection(node_id, peer_addresses)
        
        # Access tracking
        self.access_tracker = AccessTracker()
        self.heat_calculator = HeatScoreCalculator()
        
        # Track current replication factors
        self.replication_state = {}  # photo_id -> {replicas: int, last_updated: timestamp}
        self.state_lock = threading.Lock()
        
        # Flask app
        self.app = Flask(__name__)
        self.setup_routes()
        
        # Start replication monitor (only active on leader)
        threading.Thread(target=self._replication_monitor, daemon=True).start()
        
        logger.info(f"Replication Manager initialized: {node_id}")
    
    def setup_routes(self):
        """Setup Flask HTTP routes"""
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            """Health check endpoint"""
            return jsonify({
                'status': 'healthy',
                'node_id': self.node_id,
                'is_leader': self.leader_election.get_is_leader(),
                'tracked_photos': len(self.access_tracker.get_all_photo_ids())
            })
        
        @self.app.route('/report_access', methods=['POST'])
        def report_access():
            """
            Receive access report from Cache or Store.
            Called whenever a photo is read.
            """
            data = request.json
            photo_id = data.get('photo_id')
            timestamp = data.get('timestamp', time.time())
            
            if not photo_id:
                return jsonify({'error': 'photo_id required'}), 400
            
            self.access_tracker.record_access(photo_id, timestamp)
            
            return jsonify({'status': 'recorded'})
        
        @self.app.route('/get_heat_score', methods=['GET'])
        def get_heat_score():
            """Get heat score for a photo"""
            photo_id = request.args.get('photo_id')
            
            if not photo_id:
                return jsonify({'error': 'photo_id required'}), 400
            
            access_log = self.access_tracker.get_access_log(photo_id)
            heat_score = self.heat_calculator.calculate_heat_score(access_log)
            
            return jsonify({
                'photo_id': photo_id,
                'heat_score': heat_score,
                'access_count_hour': len([t for t in access_log if t >= time.time() - 3600]),
                'access_count_day': len([t for t in access_log if t >= time.time() - 86400])
            })
        
        @self.app.route('/leader_heartbeat', methods=['POST'])
        def leader_heartbeat():
            """Receive heartbeat from leader"""
            data = request.json
            leader_id = data.get('leader_id')
            self.leader_election.receive_heartbeat(leader_id)
            return jsonify({'status': 'ok'})
        
        @self.app.route('/stats', methods=['GET'])
        def get_stats():
            """Get replication manager statistics"""
            photo_ids = self.access_tracker.get_all_photo_ids()
            
            # Calculate heat distribution
            heat_scores = []
            for photo_id in photo_ids[:100]:  # Sample first 100
                access_log = self.access_tracker.get_access_log(photo_id)
                heat = self.heat_calculator.calculate_heat_score(access_log)
                heat_scores.append(heat)
            
            return jsonify({
                'node_id': self.node_id,
                'is_leader': self.leader_election.get_is_leader(),
                'tracked_photos': len(photo_ids),
                'avg_heat_score': sum(heat_scores) / len(heat_scores) if heat_scores else 0,
                'max_heat_score': max(heat_scores) if heat_scores else 0,
                'replication_operations': len(self.replication_state)
            })
    
    def _replication_monitor(self):
        """
        Background thread that monitors and adjusts replication.
        Only runs on leader node.
        """
        while True:
            try:
                time.sleep(300)  # Run every 5 minutes
                
                # Only leader performs replication decisions
                if not self.leader_election.get_is_leader():
                    continue
                
                logger.info("Starting replication analysis...")
                
                photo_ids = self.access_tracker.get_all_photo_ids()
                hot_photos = []
                cold_photos = []
                
                # Analyze heat for all photos
                for photo_id in photo_ids:
                    access_log = self.access_tracker.get_access_log(photo_id)
                    heat_score = self.heat_calculator.calculate_heat_score(access_log)
                    
                    # Get current replication factor from Directory
                    current_replicas = self._get_current_replication_factor(photo_id)
                    
                    if heat_score > self.hot_threshold and current_replicas < self.max_replicas:
                        hot_photos.append((photo_id, heat_score, current_replicas))
                    elif heat_score < self.cold_threshold and current_replicas > self.min_replicas:
                        cold_photos.append((photo_id, heat_score, current_replicas))
                
                logger.info(f"Found {len(hot_photos)} hot photos, {len(cold_photos)} cold photos")
                
                # Process hot photos - increase replication
                for photo_id, heat_score, current_replicas in hot_photos[:10]:  # Process top 10
                    target_replicas = min(current_replicas + 1, self.max_replicas)
                    self._increase_replication(photo_id, current_replicas, target_replicas)
                
                # Process cold photos - decrease replication
                for photo_id, heat_score, current_replicas in cold_photos[:5]:  # Process top 5
                    target_replicas = max(current_replicas - 1, self.min_replicas)
                    self._decrease_replication(photo_id, current_replicas, target_replicas)
                
                logger.info("Replication analysis completed")
            
            except Exception as e:
                logger.error(f"Replication monitor error: {e}")
    
    def _get_current_replication_factor(self, photo_id):
        """Get current replication factor from Directory"""
        try:
            directory_node = random.choice(self.directory_nodes)
            response = requests.get(
                f"http://{directory_node}/get_photo_location",
                params={'photo_id': photo_id},
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('replication_factor', self.min_replicas)
        except Exception as e:
            logger.error(f"Failed to get replication factor: {e}")
        
        return self.min_replicas
    
    def _increase_replication(self, photo_id, current_replicas, target_replicas):
        """Increase replication factor for a hot photo"""
        try:
            logger.info(f"Increasing replication for {photo_id}: {current_replicas} -> {target_replicas}")
            
            # Get current photo location from Directory
            directory_node = random.choice(self.directory_nodes)
            location_response = requests.get(
                f"http://{directory_node}/get_photo_location",
                params={'photo_id': photo_id},
                timeout=5
            )
            
            if location_response.status_code != 200:
                logger.error(f"Could not find photo {photo_id}")
                return
            
            location_data = location_response.json()
            existing_stores = [r['store_id'] for r in location_data['replicas']]
            source_store = location_data['replicas'][0]['address']
            
            # Request new replica from Directory (it will select appropriate store)
            # For simplicity, we'll randomly pick a store not currently hosting the photo
            # In production, Directory would use consistent hashing
            
            new_replicas_needed = target_replicas - current_replicas
            
            for _ in range(new_replicas_needed):
                # Add replica via Directory
                add_response = requests.post(
                    f"http://{directory_node}/add_replica",
                    json={
                        'photo_id': photo_id,
                        'store_id': f"store_{random.randint(4, 10)}"  # Placeholder
                    },
                    timeout=5
                )
                
                if add_response.status_code == 200:
                    logger.info(f"Successfully added replica for {photo_id}")
                    
                    # Update local state
                    with self.state_lock:
                        self.replication_state[photo_id] = {
                            'replicas': target_replicas,
                            'last_updated': time.time()
                        }
        
        except Exception as e:
            logger.error(f"Failed to increase replication for {photo_id}: {e}")
    
    def _decrease_replication(self, photo_id, current_replicas, target_replicas):
        """Decrease replication factor for a cold photo"""
        try:
            logger.info(f"Decreasing replication for {photo_id}: {current_replicas} -> {target_replicas}")
            
            # Get current replicas from Directory
            directory_node = random.choice(self.directory_nodes)
            location_response = requests.get(
                f"http://{directory_node}/get_photo_location",
                params={'photo_id': photo_id},
                timeout=5
            )
            
            if location_response.status_code != 200:
                return
            
            location_data = location_response.json()
            replicas = location_data['replicas']
            
            if len(replicas) <= self.min_replicas:
                return  # Don't go below minimum
            
            # Remove replica with lowest load (for simplicity, remove last one)
            store_to_remove = replicas[-1]['store_id']
            
            remove_response = requests.post(
                f"http://{directory_node}/remove_replica",
                json={
                    'photo_id': photo_id,
                    'store_id': store_to_remove
                },
                timeout=5
            )
            
            if remove_response.status_code == 200:
                logger.info(f"Successfully removed replica for {photo_id} from {store_to_remove}")
                
                # Update local state
                with self.state_lock:
                    self.replication_state[photo_id] = {
                        'replicas': target_replicas,
                        'last_updated': time.time()
                    }
        
        except Exception as e:
            logger.error(f"Failed to decrease replication for {photo_id}: {e}")
    
    def run(self, port):
        """Start the Flask HTTP server"""
        self.app.run(host='0.0.0.0', port=port, threaded=True)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Haystack Replication Manager')
    parser.add_argument('--node-id', required=True, help='This node ID (e.g., rm_1)')
    parser.add_argument('--address', required=True, help='This node address (host:port)')
    parser.add_argument('--peers', nargs='+', default=[], help='Peer replication manager addresses')
    parser.add_argument('--directory-nodes', nargs='+', required=True, help='Directory addresses')
    parser.add_argument('--port', type=int, required=True, help='HTTP port')
    parser.add_argument('--hot-threshold', type=int, default=100, help='Hot photo threshold')
    parser.add_argument('--cold-threshold', type=int, default=10, help='Cold photo threshold')
    
    args = parser.parse_args()
    
    manager = ReplicationManager(
        node_id=args.node_id,
        peer_addresses=args.peers,
        directory_nodes=args.directory_nodes,
        hot_threshold=args.hot_threshold,
        cold_threshold=args.cold_threshold
    )
    
    logger.info(f"Starting Replication Manager on port {args.port}")
    manager.run(args.port)


if __name__ == '__main__':
    main()
