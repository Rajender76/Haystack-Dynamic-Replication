# store_service.py


from flask import Flask, request, jsonify
import os
import struct
import hashlib
import threading
import time
import logging
import requests
import json
from collections import defaultdict
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Needle:
  
    HEADER_SIZE = 25  # 8 + 8 + 1 + 4 + 4
    FLAG_DELETED = 1
    
    def __init__(self, key, alt_key, data, flags=0):
        self.key = key
        self.alt_key = alt_key
        self.flags = flags
        self.size = len(data)
        self.data = data
        self.checksum = self._compute_checksum(data)
    
    def _compute_checksum(self, data):
        """Compute CRC32 checksum for data"""
        return hashlib.md5(data).digest()[:4]
    
    def serialize(self):
        """Serialize needle to bytes for writing to file"""
        header = struct.pack(
            '>QQBIq',  # Q=unsigned long long, B=unsigned char, I=unsigned int
            int(self.key, 16) if isinstance(self.key, str) else self.key,
            int(self.alt_key, 16) if isinstance(self.alt_key, str) else self.alt_key,
            self.flags,
            self.size,
            0  # padding
        )
        return header + self.data + self.checksum
    
    @staticmethod
    def deserialize(data):
        """Deserialize needle from bytes"""
        if len(data) < Needle.HEADER_SIZE:
            raise ValueError("Invalid needle data")
        
        key, alt_key, flags, size, _ = struct.unpack('>QQBIq', data[:Needle.HEADER_SIZE])
        photo_data = data[Needle.HEADER_SIZE:Needle.HEADER_SIZE + size]
        checksum = data[Needle.HEADER_SIZE + size:Needle.HEADER_SIZE + size + 4]
        
        needle = Needle(key, alt_key, photo_data, flags)
        
        # Verify checksum
        if needle.checksum != checksum:
            raise ValueError("Checksum mismatch")
        
        return needle
    
    def get_total_size(self):
        """Get total size of serialized needle"""
        return Needle.HEADER_SIZE + self.size + 4


class VolumeFile:
    """
    Represents a physical volume file storing multiple needles.
    Append-only file with in-memory index.
    """
    def __init__(self, volume_id, base_path, max_size_gb=10):
        self.volume_id = volume_id
        self.base_path = Path(base_path)
        self.max_size_bytes = max_size_gb * 1024 * 1024 * 1024
        
        # Create base directory if not exists
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Volume file path
        self.file_path = self.base_path / f"{volume_id}.dat"
        self.index_path = self.base_path / f"{volume_id}.idx"
        
        # In-memory index: (key, alt_key) -> (offset, size, flags)
        self.index = {}
        self.index_lock = threading.Lock()
        
        # File handle
        self.file_handle = None
        self.file_lock = threading.Lock()
        
        # Statistics
        self.write_count = 0
        self.read_count = 0
        self.current_size = 0
        
        # Initialize
        self._initialize()
    
    def _initialize(self):
        """Initialize volume file and load index"""
        # Open or create volume file
        if self.file_path.exists():
            self.file_handle = open(self.file_path, 'r+b')
            self.current_size = os.path.getsize(self.file_path)
            logger.info(f"Opened existing volume {self.volume_id}, size: {self.current_size} bytes")
            
            # Load index from file or rebuild
            if self.index_path.exists():
                self._load_index()
            else:
                self._rebuild_index()
        else:
            self.file_handle = open(self.file_path, 'w+b')
            self.current_size = 0
            logger.info(f"Created new volume {self.volume_id}")
    
    def _load_index(self):
        """Load index from disk"""
        try:
            with open(self.index_path, 'r') as f:
                self.index = json.load(f)
                # Convert string keys back to tuples
                self.index = {eval(k): v for k, v in self.index.items()}
            logger.info(f"Loaded index for {self.volume_id}: {len(self.index)} entries")
        except Exception as e:
            logger.error(f"Failed to load index: {e}, rebuilding...")
            self._rebuild_index()
    
    def _rebuild_index(self):
        """Rebuild index by scanning entire volume file"""
        logger.info(f"Rebuilding index for {self.volume_id}...")
        
        with self.index_lock:
            self.index.clear()
            offset = 0
            
            with self.file_lock:
                self.file_handle.seek(0)
                
                while offset < self.current_size:
                    try:
                        # Read needle header
                        header_data = self.file_handle.read(Needle.HEADER_SIZE)
                        if len(header_data) < Needle.HEADER_SIZE:
                            break
                        
                        key, alt_key, flags, size, _ = struct.unpack('>QQBIq', header_data)
                        
                        # Store in index
                        self.index[(key, alt_key)] = {
                            'offset': offset,
                            'size': size,
                            'flags': flags
                        }
                        
                        # Move to next needle
                        next_offset = offset + Needle.HEADER_SIZE + size + 4
                        self.file_handle.seek(next_offset)
                        offset = next_offset
                        
                    except Exception as e:
                        logger.error(f"Error rebuilding index at offset {offset}: {e}")
                        break
        
        logger.info(f"Index rebuilt: {len(self.index)} entries")
        self._save_index()
    
    def _save_index(self):
        """Persist index to disk"""
        try:
            with open(self.index_path, 'w') as f:
                # Convert tuple keys to strings for JSON
                index_json = {str(k): v for k, v in self.index.items()}
                json.dump(index_json, f)
        except Exception as e:
            logger.error(f"Failed to save index: {e}")
    
    def write(self, needle):
      
        with self.file_lock:
            # Check if volume has space
            needle_size = needle.get_total_size()
            if self.current_size + needle_size > self.max_size_bytes:
                logger.warning(f"Volume {self.volume_id} is full")
                return None
            
            # Get current offset
            offset = self.current_size
            
            # Write needle
            self.file_handle.seek(offset)
            serialized = needle.serialize()
            self.file_handle.write(serialized)
            self.file_handle.flush()
            os.fsync(self.file_handle.fileno())
            
            # Update index
            with self.index_lock:
                key_tuple = (
                    int(needle.key, 16) if isinstance(needle.key, str) else needle.key,
                    int(needle.alt_key, 16) if isinstance(needle.alt_key, str) else needle.alt_key
                )
                self.index[key_tuple] = {
                    'offset': offset,
                    'size': needle.size,
                    'flags': needle.flags
                }
            
            # Update size
            self.current_size += needle_size
            self.write_count += 1
            
            # Periodically save index
            if self.write_count % 100 == 0:
                self._save_index()
            
            return offset
    
    def read(self, key, alt_key):
    
        key_tuple = (
            int(key, 16) if isinstance(key, str) else key,
            int(alt_key, 16) if isinstance(alt_key, str) else alt_key
        )
        
       
        with self.index_lock:
            index_entry = self.index.get(key_tuple)
        
        if not index_entry:
            return None
        
   
        if index_entry['flags'] & Needle.FLAG_DELETED:
            return None
        
   
        with self.file_lock:
            offset = index_entry['offset']
            size = index_entry['size']
            total_size = Needle.HEADER_SIZE + size + 4
            
            self.file_handle.seek(offset)
            data = self.file_handle.read(total_size)
            
            if len(data) < total_size:
                logger.error(f"Incomplete read for key {key}")
                return None
        
   
        try:
            needle = Needle.deserialize(data)
            self.read_count += 1
            return needle.data
        except Exception as e:
            logger.error(f"Failed to deserialize needle: {e}")
            return None
    
    def delete(self, key, alt_key):
       
        key_tuple = (
            int(key, 16) if isinstance(key, str) else key,
            int(alt_key, 16) if isinstance(alt_key, str) else alt_key
        )
        
        with self.index_lock:
            if key_tuple in self.index:
                self.index[key_tuple]['flags'] |= Needle.FLAG_DELETED
                self._save_index()
                return True
        
        return False
    
    def get_stats(self):
        """Get volume statistics"""
        with self.index_lock:
            return {
                'volume_id': self.volume_id,
                'size_mb': self.current_size / (1024 * 1024),
                'capacity_mb': self.max_size_bytes / (1024 * 1024),
                'utilization': f"{(self.current_size / self.max_size_bytes * 100):.2f}%",
                'needle_count': len(self.index),
                'write_count': self.write_count,
                'read_count': self.read_count
            }
    
    def close(self):
        """Close volume file"""
        self._save_index()
        if self.file_handle:
            self.file_handle.close()


class StoreServer:
 
    
    def __init__(self, store_id, address, data_path, directory_nodes, 
                 replication_manager_nodes, capacity_gb=100):
      
        self.store_id = store_id
        self.address = address
        self.data_path = Path(data_path)
        self.directory_nodes = directory_nodes
        self.replication_manager_nodes = replication_manager_nodes
        self.capacity_gb = capacity_gb
        
     
        self.volumes = {}
        self.volumes_lock = threading.Lock()
        
      
        self.pending_transactions = {} 
        self.txn_lock = threading.Lock()
        
    
        self.app = Flask(__name__)
        self.setup_routes()
        
        threading.Thread(target=self._heartbeat_sender, daemon=True).start()
        
       
        self._register_with_directory()
        
        logger.info(f"Store server initialized: {store_id} at {address}")
    
    def _register_with_directory(self):
        """Register this store with Directory service"""
        for directory_node in self.directory_nodes:
            try:
                response = requests.post(
                    f"http://{directory_node}/register_store",
                    json={
                        'store_id': self.store_id,
                        'address': self.address,
                        'capacity_gb': self.capacity_gb
                    },
                    timeout=5
                )
                
                if response.status_code == 200:
                    logger.info(f"Registered with Directory: {directory_node}")
                    return
            except Exception as e:
                logger.warning(f"Failed to register with {directory_node}: {e}")
        
        logger.error("Failed to register with any Directory node")
    
    def _heartbeat_sender(self):
        """Send periodic heartbeats to Directory"""
        while True:
            try:
                time.sleep(10) 

                for directory_node in self.directory_nodes:
                    try:
                        requests.post(
                            f"http://{directory_node}/heartbeat",
                            json={'store_id': self.store_id},
                            timeout=2
                        )
                    except:
                        pass
            
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
    
    def setup_routes(self):
        """Setup Flask HTTP routes"""
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            """Health check endpoint"""
            return jsonify({
                'status': 'healthy',
                'store_id': self.store_id,
                'address': self.address,
                'volumes': len(self.volumes)
            })
        
        @self.app.route('/write', methods=['POST'])
        def write_photo():
            """
            Write photo with quorum replication (primary-backup).
            Implements two-phase commit.
            """
            data = request.json
            photo_id = data.get('photo_id')
            volume_id = data.get('volume_id')
            replica_stores = data.get('replica_stores', [])
            photo_data = data.get('data')
            metadata = data.get('metadata', {})
            
            if not all([photo_id, volume_id, photo_data]):
                return jsonify({'error': 'Missing required fields'}), 400
            
            if isinstance(photo_data, str):
                photo_data = bytes.fromhex(photo_data)
            
         
            if replica_stores and replica_stores[0] == self.store_id:
                return self._primary_write(photo_id, volume_id, photo_data, replica_stores)
            else:
                return self._backup_write(photo_id, volume_id, photo_data)
        
        @self.app.route('/prepare', methods=['POST'])
        def prepare_write():
            data = request.json
            txn_id = data.get('txn_id')
            photo_id = data.get('photo_id')
            volume_id = data.get('volume_id')
            photo_data = data.get('data')
            
            if isinstance(photo_data, str):
                photo_data = bytes.fromhex(photo_data)
            
            # Buffer the write
            with self.txn_lock:
                self.pending_transactions[txn_id] = {
                    'state': 'prepared',
                    'photo_id': photo_id,
                    'volume_id': volume_id,
                    'data': photo_data,
                    'timestamp': time.time()
                }
            
            logger.info(f"Prepared transaction {txn_id}")
            return jsonify({'status': 'prepared'})
        
        @self.app.route('/commit', methods=['POST'])
        def commit_write():
            """Phase 2 of two-phase commit: commit"""
            data = request.json
            txn_id = data.get('txn_id')
            
            with self.txn_lock:
                if txn_id not in self.pending_transactions:
                    return jsonify({'error': 'Transaction not found'}), 404
                
                txn = self.pending_transactions[txn_id]
                
                # Perform actual write
                success = self._write_to_volume(
                    txn['photo_id'],
                    txn['volume_id'],
                    txn['data']
                )
                
                # Clean up transaction
                del self.pending_transactions[txn_id]
            
            if success:
                logger.info(f"Committed transaction {txn_id}")
                return jsonify({'status': 'committed'})
            else:
                return jsonify({'error': 'Commit failed'}), 500
        
        @self.app.route('/read', methods=['GET'])
        def read_photo():
            """Read photo from store"""
            photo_id = request.args.get('photo_id')
            volume_id = request.args.get('volume_id')
            
            if not photo_id:
                return jsonify({'error': 'photo_id required'}), 400
            
            if not volume_id:
                volume_id = self._find_volume_for_photo(photo_id)
                if not volume_id:
                    return jsonify({'error': 'Photo not found'}), 404
            
            # Read from volume
            data = self._read_from_volume(photo_id, volume_id)
            
            if data:
                self._report_access(photo_id)
                
                return jsonify({
                    'photo_id': photo_id,
                    'data': data.hex(),
                    'store_id': self.store_id
                })
            
            return jsonify({'error': 'Photo not found'}), 404
        
        @self.app.route('/replicate', methods=['POST'])
        def replicate_photo():
            """Replicate photo from another store (for dynamic replication)"""
            data = request.json
            photo_id = data.get('photo_id')
            source_store = data.get('source_store')
            volume_id = data.get('volume_id')
            
            # Fetch photo from source store
            try:
                response = requests.get(
                    f"http://{source_store}/read",
                    params={'photo_id': photo_id, 'volume_id': volume_id},
                    timeout=10
                )
                
                if response.status_code == 200:
                    photo_data = bytes.fromhex(response.json()['data'])
                    
                    # Write to local volume
                    success = self._write_to_volume(photo_id, volume_id, photo_data)
                    
                    if success:
                        logger.info(f"Replicated {photo_id} from {source_store}")
                        return jsonify({'status': 'success'})
            
            except Exception as e:
                logger.error(f"Replication failed: {e}")
            
            return jsonify({'error': 'Replication failed'}), 500
        
        @self.app.route('/stats', methods=['GET'])
        def get_stats():
            """Get store statistics"""
            volume_stats = []
            with self.volumes_lock:
                for volume in self.volumes.values():
                    volume_stats.append(volume.get_stats())
            
            return jsonify({
                'store_id': self.store_id,
                'address': self.address,
                'volumes': volume_stats,
                'total_volumes': len(self.volumes)
            })
    
    def _primary_write(self, photo_id, volume_id, photo_data, replica_stores):
        """Primary store: coordinate two-phase commit with backups"""
        txn_id = hashlib.md5(f"{photo_id}:{time.time()}".encode()).hexdigest()
        
        # Phase 1: Prepare on all backups
        backup_stores = [s for s in replica_stores if s != self.store_id]
        prepared_stores = []
        
        for backup in backup_stores:
            try:
                response = requests.post(
                    f"http://{backup}/prepare",
                    json={
                        'txn_id': txn_id,
                        'photo_id': photo_id,
                        'volume_id': volume_id,
                        'data': photo_data.hex()
                    },
                    timeout=5
                )
                
                if response.status_code == 200:
                    prepared_stores.append(backup)
            except Exception as e:
                logger.warning(f"Prepare failed on {backup}: {e}")
        
        # Check quorum (need majority)
        quorum = len(replica_stores) // 2 + 1
        if len(prepared_stores) + 1 < quorum:  # +1 for primary
            logger.error(f"Quorum not reached: {len(prepared_stores) + 1}/{len(replica_stores)}")
            return jsonify({'error': 'Quorum not reached'}), 500
        
        # Phase 2: Commit on primary first
        success = self._write_to_volume(photo_id, volume_id, photo_data)
        
        if not success:
            return jsonify({'error': 'Primary write failed'}), 500
        
        # Commit on backups (best effort, asynchronous)
        for backup in prepared_stores:
            threading.Thread(
                target=self._send_commit,
                args=(backup, txn_id)
            ).start()
        
        # Notify cache
        self._notify_cache(photo_id, volume_id, photo_data)
        
        logger.info(f"Write completed: {photo_id} with {len(prepared_stores) + 1} replicas")
        
        return jsonify({
            'status': 'success',
            'photo_id': photo_id,
            'replicas': len(prepared_stores) + 1
        })
    
    def _backup_write(self, photo_id, volume_id, photo_data):
        """Backup store: simple write"""
        success = self._write_to_volume(photo_id, volume_id, photo_data)
        
        if success:
            return jsonify({'status': 'success'})
        else:
            return jsonify({'error': 'Write failed'}), 500
    
    def _send_commit(self, store_address, txn_id):
        """Send commit to backup store"""
        try:
            requests.post(
                f"http://{store_address}/commit",
                json={'txn_id': txn_id},
                timeout=5
            )
        except Exception as e:
            logger.error(f"Commit to {store_address} failed: {e}")
    
    def _write_to_volume(self, photo_id, volume_id, photo_data):
        """Write photo to volume file"""
        # Get or create volume
        with self.volumes_lock:
            if volume_id not in self.volumes:
                self.volumes[volume_id] = VolumeFile(volume_id, self.data_path)
            volume = self.volumes[volume_id]
        
        # Create needle
        key = int(hashlib.md5(photo_id.encode()).hexdigest()[:16], 16)
        alt_key = int(hashlib.md5(f"{photo_id}:alt".encode()).hexdigest()[:16], 16)
        needle = Needle(key, alt_key, photo_data)
        
        # Write
        offset = volume.write(needle)
        return offset is not None
    
    def _read_from_volume(self, photo_id, volume_id):
        """Read photo from volume file"""
        with self.volumes_lock:
            if volume_id not in self.volumes:
                return None
            volume = self.volumes[volume_id]
        
        key = int(hashlib.md5(photo_id.encode()).hexdigest()[:16], 16)
        alt_key = int(hashlib.md5(f"{photo_id}:alt".encode()).hexdigest()[:16], 16)
        
        return volume.read(key, alt_key)
    
    def _find_volume_for_photo(self, photo_id):
        """Search all volumes for a photo (slow)"""
        with self.volumes_lock:
            for volume_id, volume in self.volumes.items():
                key = int(hashlib.md5(photo_id.encode()).hexdigest()[:16], 16)
                alt_key = int(hashlib.md5(f"{photo_id}:alt".encode()).hexdigest()[:16], 16)
                
                if volume.read(key, alt_key):
                    return volume_id
        return None
    
    def _notify_cache(self, photo_id, volume_id, photo_data):
        """Notify cache about new photo"""
        # In production, get cache addresses from Directory
        # For simplicity, hardcode or skip
        pass
    
    def _report_access(self, photo_id):
        """Report photo access to Replication Manager"""
        if not self.replication_manager_nodes:
            return
        
        # Send asynchronously
        def send_report():
            try:
                rm_node = self.replication_manager_nodes[0]
                requests.post(
                    f"http://{rm_node}/report_access",
                    json={'photo_id': photo_id, 'timestamp': time.time()},
                    timeout=1
                )
            except:
                pass
        
        threading.Thread(target=send_report).start()
    
    def run(self, port):
        """Start the Flask HTTP server"""
        self.app.run(host='0.0.0.0', port=port, threaded=True)
    
    def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down store...")
        with self.volumes_lock:
            for volume in self.volumes.values():
                volume.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Haystack Store Service')
    parser.add_argument('--store-id', required=True, help='Store ID (e.g., store_1)')
    parser.add_argument('--address', required=True, help='This store address (host:port)')
    parser.add_argument('--port', type=int, required=True, help='HTTP port')
    parser.add_argument('--data-path', required=True, help='Data directory path')
    parser.add_argument('--directory-nodes', nargs='+', required=True, help='Directory addresses')
    parser.add_argument('--rm-nodes', nargs='+', default=[], help='Replication Manager addresses')
    parser.add_argument('--capacity', type=int, default=100, help='Capacity in GB')
    
    args = parser.parse_args()
    
    server = StoreServer(
        store_id=args.store_id,
        address=args.address,
        data_path=args.data_path,
        directory_nodes=args.directory_nodes,
        replication_manager_nodes=args.rm_nodes,
        capacity_gb=args.capacity
    )
    
    logger.info(f"Starting Store server on port {args.port}")
    
    try:
        server.run(args.port)
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == '__main__':
    main()
