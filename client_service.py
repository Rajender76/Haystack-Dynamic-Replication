# client_service.py
"""
End Client Service for Distributed Haystack
"""

import requests
import hashlib
import time
import random
import os
import json
import numpy as np
from collections import defaultdict
from datetime import datetime
import threading
import logging
from enum import Enum

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      
    OPEN = "open"          
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """
    Circuit Breaker pattern implementation to prevent cascading failures.
    Opens circuit after threshold failures, closes after recovery period.
    """
    def __init__(self, failure_threshold=5, recovery_timeout=30, success_threshold=2):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout  # seconds
        self.success_threshold = success_threshold
        
        self.failure_count = 0
        self.success_count = 0
        self.state = CircuitState.CLOSED
        self.last_failure_time = None
        self.lock = threading.Lock()
    
    def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        with self.lock:
            if self.state == CircuitState.OPEN:
                # Check if recovery timeout has passed
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    logger.info("Circuit breaker: OPEN -> HALF_OPEN")
                else:
                    raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e
    
    def _on_success(self):
        """Handle successful request"""
        with self.lock:
            self.failure_count = 0
            
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.success_threshold:
                    self.state = CircuitState.CLOSED
                    logger.info("Circuit breaker: HALF_OPEN -> CLOSED")
    
    def _on_failure(self):
        """Handle failed request"""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = CircuitState.OPEN
                logger.warning(f"Circuit breaker: -> OPEN (failures: {self.failure_count})")


class LoadBalancer:
    """
    Client-side load balancer for distributing requests across Directory nodes.
    Uses round-robin with health checking.
    """
    def __init__(self, directory_nodes):
        """
        Args:
            directory_nodes: List of directory addresses ['host1:port1', 'host2:port2']
        """
        self.nodes = directory_nodes
        self.current_index = 0
        self.node_health = {node: True for node in directory_nodes}
        self.lock = threading.Lock()
    
    def get_next_node(self):
        """Get next healthy node using round-robin"""
        with self.lock:
            attempts = 0
            while attempts < len(self.nodes):
                node = self.nodes[self.current_index]
                self.current_index = (self.current_index + 1) % len(self.nodes)
                
                if self.node_health[node]:
                    return node
                attempts += 1
            
            # All nodes unhealthy, return first one anyway
            logger.warning("All directory nodes marked unhealthy, using first node")
            return self.nodes[0]
    
    def mark_node_unhealthy(self, node):
        """Mark a node as unhealthy"""
        with self.lock:
            self.node_health[node] = False
            logger.warning(f"Marked node {node} as unhealthy")
    
    def mark_node_healthy(self, node):
        """Mark a node as healthy"""
        with self.lock:
            self.node_health[node] = True
            logger.info(f"Marked node {node} as healthy")


class MetricsCollector:
    """Collect and track client-side metrics"""
    def __init__(self):
        self.upload_count = 0
        self.read_count = 0
        self.upload_latencies = []
        self.read_latencies = []
        self.errors = defaultdict(int)
        self.lock = threading.Lock()
    
    def record_upload(self, latency, success=True):
        with self.lock:
            self.upload_count += 1
            if success:
                self.upload_latencies.append(latency)
            else:
                self.errors['upload'] += 1
    
    def record_read(self, latency, success=True):
        with self.lock:
            self.read_count += 1
            if success:
                self.read_latencies.append(latency)
            else:
                self.errors['read'] += 1
    
    def get_stats(self):
        """Get current statistics"""
        with self.lock:
            return {
                'upload_count': self.upload_count,
                'read_count': self.read_count,
                'avg_upload_latency': np.mean(self.upload_latencies) if self.upload_latencies else 0,
                'avg_read_latency': np.mean(self.read_latencies) if self.read_latencies else 0,
                'p95_upload_latency': np.percentile(self.upload_latencies, 95) if self.upload_latencies else 0,
                'p95_read_latency': np.percentile(self.read_latencies, 95) if self.read_latencies else 0,
                'errors': dict(self.errors)
            }
    
    def print_stats(self):
        """Print formatted statistics"""
        stats = self.get_stats()
        print("\n" + "="*60)
        print("CLIENT METRICS")
        print("="*60)
        print(f"Total Uploads: {stats['upload_count']}")
        print(f"Total Reads: {stats['read_count']}")
        print(f"Avg Upload Latency: {stats['avg_upload_latency']:.2f}ms")
        print(f"Avg Read Latency: {stats['avg_read_latency']:.2f}ms")
        print(f"P95 Upload Latency: {stats['p95_upload_latency']:.2f}ms")
        print(f"P95 Read Latency: {stats['p95_read_latency']:.2f}ms")
        print(f"Errors: {stats['errors']}")
        print("="*60 + "\n")


class HaystackClient:
    """
    Main client for interacting with Distributed Haystack system.
    Handles uploads, reads, and implements distributed systems patterns.
    """
    
    def __init__(self, directory_nodes, cache_nodes, client_id=None):
        """
        Args:
            directory_nodes: List of Directory service addresses
            cache_nodes: List of Cache service addresses
            client_id: Unique identifier for this client instance
        """
        self.client_id = client_id or f"client_{random.randint(1000, 9999)}"
        self.directory_lb = LoadBalancer(directory_nodes)
        self.cache_nodes = cache_nodes
        self.circuit_breaker = CircuitBreaker()
        self.metrics = MetricsCollector()
        self.session = requests.Session()  # Connection pooling
        
        # Local cache of uploaded photos for generating reads
        self.uploaded_photos = []
        self.lock = threading.Lock()
        
        logger.info(f"HaystackClient {self.client_id} initialized")
    
    def _exponential_backoff_retry(self, func, max_retries=3, *args, **kwargs):
        """
        Execute function with exponential backoff retry logic.
        Waits exponentially longer between retries: 1s, 2s, 4s, etc.
        """
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed after {max_retries} attempts: {e}")
                    raise
                
                wait_time = 2 ** attempt + random.uniform(0, 1)
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time:.2f}s")
                time.sleep(wait_time)
    
    def _make_request_with_retry(self, method, url, **kwargs):
        """Make HTTP request with retry logic"""
        def make_request():
            timeout = kwargs.pop('timeout', 5)
            response = self.session.request(method, url, timeout=timeout, **kwargs)
            response.raise_for_status()
            return response
        
        return self._exponential_backoff_retry(make_request)
    
    def generate_photo_id(self):
        """Generate unique photo ID"""
        timestamp = str(time.time())
        random_bytes = os.urandom(8)
        combined = f"{self.client_id}_{timestamp}_{random_bytes.hex()}"
        return hashlib.md5(combined.encode()).hexdigest()
    
    def upload_photo(self, photo_data, metadata=None):
        """
        Upload a photo to Haystack system.
        
        Args:
            photo_data: Binary photo data (bytes)
            metadata: Optional dict with photo metadata
        
        Returns:
            dict: {'photo_id': str, 'url': str, 'latency_ms': float}
        """
        start_time = time.time()
        photo_id = self.generate_photo_id()
        
        try:
            # Step 1: Get volume assignment from Directory (with circuit breaker)
            def get_volume_assignment():
                directory_node = self.directory_lb.get_next_node()
                url = f"http://{directory_node}/assign_volume"
                
                response = self._make_request_with_retry(
                    'POST',
                    url,
                    json={'photo_id': photo_id},
                    timeout=5
                )
                return response.json(), directory_node
            
            assignment, directory_node = self.circuit_breaker.call(get_volume_assignment)
            self.directory_lb.mark_node_healthy(directory_node)
            
            volume_id = assignment['volume_id']
            primary_store = assignment['primary']
            replica_stores = assignment['replicas']
            
            logger.info(f"Photo {photo_id} assigned to volume {volume_id}, primary: {primary_store}")
            
            # Step 2: Write to primary store with quorum replication
            primary_url = f"http://{primary_store}/write"
            
            write_payload = {
                'photo_id': photo_id,
                'volume_id': volume_id,
                'replica_stores': replica_stores,
                'metadata': metadata or {},
                'data': photo_data.hex()  # Convert bytes to hex string for JSON
            }
            
            response = self._make_request_with_retry(
                'POST',
                primary_url,
                json=write_payload,
                timeout=10
            )
            
            result = response.json()
            
            # Step 3: Store photo info locally for generating reads later
            with self.lock:
                self.uploaded_photos.append({
                    'photo_id': photo_id,
                    'volume_id': volume_id,
                    'replicas': replica_stores,
                    'upload_time': time.time()
                })
            
            latency_ms = (time.time() - start_time) * 1000
            self.metrics.record_upload(latency_ms, success=True)
            
            logger.info(f"Upload successful: {photo_id} in {latency_ms:.2f}ms")
            
            return {
                'photo_id': photo_id,
                'url': f"/photo/{photo_id}",
                'latency_ms': latency_ms,
                'replicas': len(replica_stores)
            }
            
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            self.metrics.record_upload(latency_ms, success=False)
            logger.error(f"Upload failed for {photo_id}: {e}")
            raise
    
    def read_photo(self, photo_id, use_hedged_requests=True):
        """
        Read a photo from Haystack system.
        
        Args:
            photo_id: Photo identifier
            use_hedged_requests: Whether to use hedged requests for fault tolerance
        
        Returns:
            dict: {'photo_id': str, 'data': bytes, 'latency_ms': float, 'source': str}
        """
        start_time = time.time()
        
        try:
            # Step 1: Get photo location from Directory
            directory_node = self.directory_lb.get_next_node()
            location_url = f"http://{directory_node}/get_photo_location"
            
            response = self._make_request_with_retry(
                'GET',
                location_url,
                params={'photo_id': photo_id},
                timeout=5
            )
            
            location_data = response.json()
            replica_addresses = location_data['replicas']
            
            if not replica_addresses:
                raise Exception(f"No replicas found for photo {photo_id}")
            
            # Step 2: Try cache first
            cache_result = self._try_cache(photo_id)
            if cache_result:
                latency_ms = (time.time() - start_time) * 1000
                self.metrics.record_read(latency_ms, success=True)
                logger.info(f"Cache HIT for {photo_id} in {latency_ms:.2f}ms")
                return {
                    'photo_id': photo_id,
                    'data': cache_result,
                    'latency_ms': latency_ms,
                    'source': 'cache'
                }
            
            # Step 3: Cache miss - read from Store
            if use_hedged_requests and len(replica_addresses) > 1:
                photo_data = self._read_with_hedged_requests(photo_id, replica_addresses)
            else:
                # Simple read from first replica
                store_address = replica_addresses[0]['address']
                photo_data = self._read_from_store(photo_id, store_address)
            
            latency_ms = (time.time() - start_time) * 1000
            self.metrics.record_read(latency_ms, success=True)
            
            logger.info(f"Read successful: {photo_id} in {latency_ms:.2f}ms from store")
            
            return {
                'photo_id': photo_id,
                'data': photo_data,
                'latency_ms': latency_ms,
                'source': 'store'
            }
            
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            self.metrics.record_read(latency_ms, success=False)
            logger.error(f"Read failed for {photo_id}: {e}")
            raise
    
    def _try_cache(self, photo_id):
        """Try to read from cache using consistent hashing"""
        try:
            # Use consistent hashing to determine which cache node
            cache_index = int(hashlib.md5(photo_id.encode()).hexdigest(), 16) % len(self.cache_nodes)
            cache_node = self.cache_nodes[cache_index]
            
            url = f"http://{cache_node}/get"
            response = self.session.get(url, params={'photo_id': photo_id}, timeout=2)
            
            if response.status_code == 200:
                return bytes.fromhex(response.json()['data'])
            return None
            
        except Exception as e:
            logger.debug(f"Cache miss or error: {e}")
            return None
    
    def _read_from_store(self, photo_id, store_address):
        """Read photo from specific store machine"""
        url = f"http://{store_address}/read"
        response = self._make_request_with_retry(
            'GET',
            url,
            params={'photo_id': photo_id},
            timeout=5
        )
        
        result = response.json()
        return bytes.fromhex(result['data'])
    
    def _read_with_hedged_requests(self, photo_id, replica_addresses, hedge_delay_ms=100):
        """
        Read with hedged requests pattern for improved latency.
        Sends request to primary, if it doesn't respond within hedge_delay,
        send parallel request to backup replica.
        """
        primary_store = replica_addresses[0]['address']
        backup_store = replica_addresses[1]['address'] if len(replica_addresses) > 1 else None
        
        result = {'data': None, 'error': None}
        result_lock = threading.Lock()
        result_event = threading.Event()
        
        def read_from_replica(store_address, is_primary=False):
            """Thread function to read from a replica"""
            try:
                photo_data = self._read_from_store(photo_id, store_address)
                with result_lock:
                    if result['data'] is None:
                        result['data'] = photo_data
                        result_event.set()
            except Exception as e:
                with result_lock:
                    if result['error'] is None:
                        result['error'] = e
        
        # Start primary request
        primary_thread = threading.Thread(
            target=read_from_replica,
            args=(primary_store, True)
        )
        primary_thread.start()
        
        # Wait for hedge delay
        time.sleep(hedge_delay_ms / 1000.0)
        
        # If no result yet and we have backup, send hedged request
        if not result_event.is_set() and backup_store:
            logger.debug(f"Sending hedged request for {photo_id} to {backup_store}")
            backup_thread = threading.Thread(
                target=read_from_replica,
                args=(backup_store, False)
            )
            backup_thread.start()
        
        # Wait for result (max 5 seconds)
        result_event.wait(timeout=5)
        
        if result['data']:
            return result['data']
        elif result['error']:
            raise result['error']
        else:
            raise Exception("Hedged request timeout")
    
    def generate_workload(self, num_photos=100, read_ratio=0.8, duration_seconds=60, 
                         zipf_param=1.5, hot_photo_ratio=0.2):
        """
        Generate realistic workload with Zipf distribution for testing.
        20% of photos get 80% of traffic (hot photos).
        
        Args:
            num_photos: Number of photos to upload initially
            read_ratio: Ratio of reads to total operations (0.8 = 80% reads)
            duration_seconds: How long to generate load
            zipf_param: Zipf distribution parameter (higher = more skewed)
            hot_photo_ratio: Ratio of photos considered "hot"
        """
        logger.info(f"Starting workload generation: {num_photos} photos, {duration_seconds}s duration")
        
        # Phase 1: Upload initial photos
        print(f"\nUploading {num_photos} initial photos...")
        for i in range(num_photos):
            photo_data = os.urandom(1024 * random.randint(10, 100))  # 10-100KB photos
            metadata = {
                'upload_time': datetime.now().isoformat(),
                'size': len(photo_data),
                'index': i
            }
            
            try:
                result = self.upload_photo(photo_data, metadata)
                if (i + 1) % 10 == 0:
                    print(f"Uploaded {i + 1}/{num_photos} photos")
            except Exception as e:
                logger.error(f"Upload {i} failed: {e}")
        
        print(f"Completed uploading {num_photos} photos\n")
        
        # Phase 2: Generate read/write traffic with Zipf distribution
        print(f"Generating mixed workload for {duration_seconds}s...")
        start_time = time.time()
        operation_count = 0
        
        with self.lock:
            photo_list = self.uploaded_photos.copy()
        
        # Generate Zipf distribution for photo access
        zipf_dist = np.random.zipf(zipf_param, size=10000)
        zipf_dist = (zipf_dist - 1) % len(photo_list)  # Map to photo indices
        
        while time.time() - start_time < duration_seconds:
            operation_count += 1
            
            # Decide operation type based on read_ratio
            if random.random() < read_ratio:
                # Read operation - use Zipf distribution
                photo_index = zipf_dist[operation_count % len(zipf_dist)]
                photo = photo_list[photo_index]
                
                try:
                    self.read_photo(photo['photo_id'])
                except Exception as e:
                    logger.debug(f"Read operation failed: {e}")
            else:
                # Write operation
                photo_data = os.urandom(1024 * random.randint(10, 100))
                try:
                    result = self.upload_photo(photo_data)
                    with self.lock:
                        self.uploaded_photos.append({
                            'photo_id': result['photo_id'],
                            'upload_time': time.time()
                        })
                        photo_list = self.uploaded_photos.copy()
                except Exception as e:
                    logger.debug(f"Write operation failed: {e}")
            
            # Small delay to prevent overwhelming the system
            time.sleep(random.uniform(0.01, 0.05))
            
            # Print progress every 100 operations
            if operation_count % 100 == 0:
                elapsed = time.time() - start_time
                print(f"Operations: {operation_count}, Elapsed: {elapsed:.1f}s")
        
        print(f"\nWorkload generation completed: {operation_count} operations\n")
        self.metrics.print_stats()


def main():
    """Example usage of HaystackClient"""
    
    # Configuration
    DIRECTORY_NODES = [
        'localhost:8001',
        'localhost:8002',
        'localhost:8003'
    ]
    
    CACHE_NODES = [
        'localhost:9001',
        'localhost:9002',
        'localhost:9003'
    ]
    
    # Create client
    client = HaystackClient(
        directory_nodes=DIRECTORY_NODES,
        cache_nodes=CACHE_NODES,
        client_id="test_client_1"
    )
    
    # Example 1: Upload a single photo
    print("\n=== Example 1: Single Photo Upload ===")
    photo_data = b"This is a test photo data" * 100
    result = client.upload_photo(photo_data, metadata={'name': 'test.jpg'})
    print(f"Uploaded: {result}")
    
    # Example 2: Read the photo back
    print("\n=== Example 2: Read Photo ===")
    read_result = client.read_photo(result['photo_id'])
    print(f"Read: photo_id={read_result['photo_id']}, "
          f"latency={read_result['latency_ms']:.2f}ms, "
          f"source={read_result['source']}")
    
    # Example 3: Generate realistic workload
    print("\n=== Example 3: Generate Workload ===")
    client.generate_workload(
        num_photos=50,          
        read_ratio=0.8,         
        duration_seconds=30,    
        zipf_param=1.5       
    )


if __name__ == '__main__':
    main()
