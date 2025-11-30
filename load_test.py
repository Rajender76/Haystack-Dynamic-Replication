#!/usr/bin/env python3
"""
Comprehensive Performance Metrics Test Script - FIXED VERSION
- Fixed cache testing logic
- Increased operation counts (1000+ ops)
- Better error handling
- More detailed metrics
"""

import requests
import time
import hashlib
import sys
import os
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
import json
from pathlib import Path

# Service URLs
DIRECTORY_URL = os.getenv("DIRECTORY_SERVICE_URL", "http://localhost:80")
CACHE_URL = os.getenv("CACHE_SERVICE_URL", "http://localhost:8100")
REPLICATION_URL = os.getenv("REPLICATION_MANAGER_URL", "http://localhost:9003")

# Docker hostname to localhost port mapping
STORE_PORT_MAPPING = {
    'store-1': 8001,
    'store-2': 8002,
    'store-3': 8003,
    'store-4': 8004,
    'store-5': 8005,
}

REQUEST_TIMEOUT = 30

# Colors
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    ENDC = '\033[0m'

def print_header(msg: str):
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*80}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{msg.center(80)}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*80}{Colors.ENDC}\n")

def print_success(msg: str):
    print(f"{Colors.GREEN}✓ {msg}{Colors.ENDC}")

def print_error(msg: str):
    print(f"{Colors.RED}✗ {msg}{Colors.ENDC}")

def print_info(msg: str):
    print(f"{Colors.BLUE}ℹ {msg}{Colors.ENDC}")

def print_warning(msg: str):
    print(f"{Colors.YELLOW}⚠ {msg}{Colors.ENDC}")

def translate_docker_url(url: str) -> str:
    """Translate Docker internal URL to localhost URL"""
    for store_name, localhost_port in STORE_PORT_MAPPING.items():
        pattern = f"http://{store_name}:8000"
        if pattern in url:
            return url.replace(pattern, f"http://localhost:{localhost_port}")
    return url

@dataclass
class LatencyMetrics:
    """Container for latency statistics"""
    measurements: List[float] = field(default_factory=list)
    
    def add(self, latency_ms: float):
        self.measurements.append(latency_ms)
    
    def get_stats(self) -> Dict:
        if not self.measurements:
            return {}
        
        arr = np.array(self.measurements)
        return {
            'count': len(self.measurements),
            'min': float(np.min(arr)),
            'max': float(np.max(arr)),
            'mean': float(np.mean(arr)),
            'median': float(np.median(arr)),
            'p50': float(np.percentile(arr, 50)),
            'p75': float(np.percentile(arr, 75)),
            'p90': float(np.percentile(arr, 90)),
            'p95': float(np.percentile(arr, 95)),
            'p99': float(np.percentile(arr, 99)),
            'stddev': float(np.std(arr))
        }

@dataclass
class OperationMetrics:
    """Metrics for a specific operation"""
    operation_name: str
    latency: LatencyMetrics = field(default_factory=LatencyMetrics)
    total_bytes: int = 0
    success_count: int = 0
    failure_count: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    store_distribution: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    
    def start(self):
        self.start_time = time.time()
    
    def end(self):
        self.end_time = time.time()
    
    def add_success(self, latency_ms: float, bytes_transferred: int = 0, store_id: str = None):
        self.success_count += 1
        self.total_bytes += bytes_transferred
        self.latency.add(latency_ms)
        if store_id:
            self.store_distribution[store_id] += 1
    
    def add_failure(self):
        self.failure_count += 1
    
    def get_duration(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0
    
    def get_throughput_mbps(self) -> float:
        duration = self.get_duration()
        if duration > 0:
            mb = self.total_bytes / (1024 * 1024)
            return (mb * 8) / duration  # Mbps
        return 0.0
    
    def get_throughput_ops_per_sec(self) -> float:
        duration = self.get_duration()
        if duration > 0:
            return self.success_count / duration
        return 0.0
    
    def print_summary(self):
        print(f"\n{Colors.BOLD}{Colors.CYAN}{'─'*80}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.CYAN}{self.operation_name} - Performance Summary{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.CYAN}{'─'*80}{Colors.ENDC}")
        
        # Success/Failure
        total = self.success_count + self.failure_count
        success_rate = (self.success_count / total * 100) if total > 0 else 0
        print(f"\n{Colors.BOLD}Operation Statistics:{Colors.ENDC}")
        print(f"  • Total operations: {total}")
        print(f"  • Successful: {self.success_count} ({success_rate:.1f}%)")
        print(f"  • Failed: {self.failure_count}")
        print(f"  • Duration: {self.get_duration():.2f}s")
        
        # Throughput
        print(f"\n{Colors.BOLD}Throughput:{Colors.ENDC}")
        print(f"  • Operations/sec: {self.get_throughput_ops_per_sec():.2f}")
        if self.total_bytes > 0:
            print(f"  • Data transferred: {self.total_bytes / (1024*1024):.2f} MB")
            print(f"  • Throughput: {self.get_throughput_mbps():.2f} Mbps")
            print(f"  • Avg bytes/op: {self.total_bytes / self.success_count if self.success_count > 0 else 0:.0f}")
        
        # Latency
        stats = self.latency.get_stats()
        if stats:
            print(f"\n{Colors.BOLD}Latency Distribution (ms):{Colors.ENDC}")
            print(f"  • Min:      {stats['min']:8.2f} ms")
            print(f"  • P50:      {stats['p50']:8.2f} ms")
            print(f"  • P75:      {stats['p75']:8.2f} ms")
            print(f"  • P90:      {stats['p90']:8.2f} ms")
            print(f"  • P95:      {stats['p95']:8.2f} ms")
            print(f"  • P99:      {stats['p99']:8.2f} ms")
            print(f"  • Max:      {stats['max']:8.2f} ms")
            print(f"  • Mean:     {stats['mean']:8.2f} ms")
            print(f"  • Std Dev:  {stats['stddev']:8.2f} ms")
        
        # Store distribution
        if self.store_distribution:
            print(f"\n{Colors.BOLD}Load Distribution:{Colors.ENDC}")
            total_ops = sum(self.store_distribution.values())
            for store_id in sorted(self.store_distribution.keys()):
                count = self.store_distribution[store_id]
                percentage = (count / total_ops * 100) if total_ops > 0 else 0
                print(f"  • {store_id}: {count:4d} ops ({percentage:5.1f}%)")

class PerformanceTester:
    """Comprehensive performance testing"""
    
    def __init__(self):
        self.directory_url = DIRECTORY_URL
        self.cache_url = CACHE_URL
        self.replication_url = REPLICATION_URL
        self.metrics = {}
        self.test_cache_service()
    
    def test_cache_service(self):
        """Test if cache service is responding"""
        print_info("Testing cache service availability...")
        try:
            response = requests.get(f"{self.cache_url}/health", timeout=5)
            if response.status_code == 200:
                print_success("Cache service is responding")
            else:
                print_warning(f"Cache service returned status {response.status_code}")
        except Exception as e:
            print_warning(f"Cache service may not be available: {e}")
            print_info("Cache tests will show misses only")
    
    def test_upload_performance(self, image_path: str, num_uploads: int = 100) -> OperationMetrics:
        """Test upload performance with multiple iterations"""
        print_header(f"UPLOAD PERFORMANCE TEST ({num_uploads} operations)")
        
        # Verify image exists
        if not os.path.exists(image_path):
            print_error(f"Image not found: {image_path}")
            sys.exit(1)
        
        with open(image_path, 'rb') as f:
            image_data = f.read()
        
        image_size_mb = len(image_data) / (1024 * 1024)
        print_info(f"Test image: {image_path}")
        print_info(f"Image size: {image_size_mb:.2f} MB ({len(image_data)} bytes)")
        print_info(f"Number of uploads: {num_uploads}")
        print()
        
        metrics = OperationMetrics("Upload Operations")
        metrics.start()
        
        uploaded_photo_ids = []
        progress_interval = max(10, num_uploads // 20)
        
        for i in range(num_uploads):
            upload_start = time.time()
            
            try:
                # Generate unique photo_id for each upload
                unique_data = image_data + f"{time.time()}-{i}".encode()
                photo_id = hashlib.sha256(unique_data).hexdigest()
                
                # Step 1: Allocate write location
                alloc_response = requests.post(
                    f"{self.directory_url}/allocate",
                    json={'photo_id': photo_id},
                    timeout=REQUEST_TIMEOUT
                )
                
                if alloc_response.status_code != 200:
                    metrics.add_failure()
                    if (i + 1) % progress_interval == 0:
                        print_error(f"  [{i+1}/{num_uploads}] Allocation failed: {alloc_response.status_code}")
                    continue
                
                allocation = alloc_response.json()
                primary_store_url = translate_docker_url(allocation['primary_store_url'])
                
                # Step 2: Write to store
                files = {'photo': ('photo.jpg', image_data, 'image/jpeg')}
                data = {'photo_id': photo_id}
                
                write_response = requests.post(
                    f"{primary_store_url}/write",
                    files=files,
                    data=data,
                    timeout=REQUEST_TIMEOUT
                )
                
                if write_response.status_code != 200:
                    metrics.add_failure()
                    if (i + 1) % progress_interval == 0:
                        print_error(f"  [{i+1}/{num_uploads}] Write failed: {write_response.status_code}")
                    continue
                
                write_result = write_response.json()
                store_id = write_result['store_id']
                volume_id = write_result['volume_id']
                
                # Step 3: Register with directory
                checksum = hashlib.sha256(image_data).hexdigest()
                register_response = requests.post(
                    f"{self.directory_url}/register",
                    json={
                        'photo_id': photo_id,
                        'store_id': store_id,
                        'volume_id': volume_id,
                        'checksum': checksum,
                        'size_bytes': len(image_data)
                    },
                    timeout=REQUEST_TIMEOUT
                )
                
                if register_response.status_code != 200:
                    metrics.add_failure()
                    if (i + 1) % progress_interval == 0:
                        print_error(f"  [{i+1}/{num_uploads}] Registration failed: {register_response.status_code}")
                    continue
                
                upload_end = time.time()
                latency_ms = (upload_end - upload_start) * 1000
                
                metrics.add_success(latency_ms, len(image_data), store_id)
                uploaded_photo_ids.append(photo_id)
                
                if (i + 1) % progress_interval == 0:
                    print_success(f"  [{i+1}/{num_uploads}] Upload complete: {latency_ms:.1f}ms → {store_id}/{volume_id}")
                
            except Exception as e:
                metrics.add_failure()
                if (i + 1) % progress_interval == 0:
                    print_error(f"  [{i+1}/{num_uploads}] Upload error: {e}")
        
        metrics.end()
        metrics.print_summary()
        
        # Store for later tests
        self.metrics['upload'] = metrics
        self.uploaded_photo_ids = uploaded_photo_ids
        
        return metrics
    
    def test_download_performance_cache_cold(self, photo_id: str, num_downloads: int = 1000) -> OperationMetrics:
        """Test download performance with cache disabled (cold reads from store)"""
        print_header(f"DOWNLOAD PERFORMANCE TEST - CACHE COLD ({num_downloads} operations)")
        
        print_info(f"Photo ID: {photo_id[:50]}...")
        print_info(f"Number of downloads: {num_downloads}")
        print_info(f"Cache: DISABLED (invalidated before each request)")
        print()
        
        metrics = OperationMetrics("Download Operations (Cache Cold)")
        metrics.start()
        
        progress_interval = max(50, num_downloads // 20)
        
        for i in range(num_downloads):
            download_start = time.time()
            
            try:
                # CRITICAL: Invalidate cache before EVERY download to ensure cold read
                try:
                    requests.delete(f"{self.cache_url}/cache/{photo_id}", timeout=2)
                except:
                    pass  # If cache delete fails, continue anyway
                
                # Step 1: Locate photo
                locate_response = requests.get(
                    f"{self.directory_url}/locate/{photo_id}",
                    timeout=10
                )
                
                if locate_response.status_code != 200:
                    metrics.add_failure()
                    continue
                
                locations = locate_response.json()['locations']
                
                # Step 2: Download from store (round-robin across replicas)
                location = locations[i % len(locations)]
                store_url = translate_docker_url(location['store_url'])
                store_id = location['store_id']
                
                read_response = requests.get(
                    f"{store_url}/read/{photo_id}",
                    timeout=10
                )
                
                if read_response.status_code != 200:
                    metrics.add_failure()
                    continue
                
                download_end = time.time()
                latency_ms = (download_end - download_start) * 1000
                
                metrics.add_success(latency_ms, len(read_response.content), store_id)
                
                if (i + 1) % progress_interval == 0:
                    print_info(f"  [{i+1}/{num_downloads}] Downloaded from {store_id}: {latency_ms:.1f}ms")
                
            except Exception as e:
                metrics.add_failure()
        
        metrics.end()
        metrics.print_summary()
        
        self.metrics['download_cold'] = metrics
        return metrics
    
    def test_download_performance_cache_warm(self, photo_id: str, num_downloads: int = 1000) -> OperationMetrics:
        """Test download performance with cache enabled - FIXED to actually use cache"""
        print_header(f"DOWNLOAD PERFORMANCE TEST - CACHE WARM ({num_downloads} operations)")
        
        print_info(f"Photo ID: {photo_id[:50]}...")
        print_info(f"Number of downloads: {num_downloads}")
        print_info(f"Cache: ENABLED (using client service flow)")
        print()
        
        # Prime the cache first by downloading once
        print_info("Priming cache with initial download...")
        try:
            # Download from store first to ensure it's in cache
            locate_response = requests.get(f"{self.directory_url}/locate/{photo_id}", timeout=10)
            if locate_response.status_code == 200:
                locations = locate_response.json()['locations']
                store_url = translate_docker_url(locations[0]['store_url'])
                requests.get(f"{store_url}/read/{photo_id}", timeout=10)
                time.sleep(0.5)  # Give cache time to update
                print_success("Cache primed")
        except Exception as e:
            print_warning(f"Cache priming failed: {e}")
        
        metrics = OperationMetrics("Download Operations (Cache Warm)")
        metrics.start()
        
        cache_hits = 0
        cache_misses = 0
        progress_interval = max(50, num_downloads // 20)
        
        for i in range(num_downloads):
            download_start = time.time()
            
            try:
                # Check cache first (this is the correct flow)
                cache_response = requests.get(
                    f"{self.cache_url}/cache/{photo_id}",
                    timeout=5
                )
                
                if cache_response.status_code == 200:
                    # Cache HIT
                    download_end = time.time()
                    latency_ms = (download_end - download_start) * 1000
                    metrics.add_success(latency_ms, len(cache_response.content), 'cache-hit')
                    cache_hits += 1
                    
                    if (i + 1) % progress_interval == 0:
                        print_success(f"  [{i+1}/{num_downloads}] Cache HIT: {latency_ms:.1f}ms")
                    
                else:
                    # Cache MISS - fallback to store
                    cache_misses += 1
                    
                    locate_response = requests.get(
                        f"{self.directory_url}/locate/{photo_id}",
                        timeout=10
                    )
                    
                    if locate_response.status_code != 200:
                        metrics.add_failure()
                        continue
                    
                    locations = locate_response.json()['locations']
                    location = locations[0]
                    store_url = translate_docker_url(location['store_url'])
                    store_id = location['store_id']
                    
                    read_response = requests.get(
                        f"{store_url}/read/{photo_id}",
                        timeout=10
                    )
                    
                    if read_response.status_code != 200:
                        metrics.add_failure()
                        continue
                    
                    download_end = time.time()
                    latency_ms = (download_end - download_start) * 1000
                    metrics.add_success(latency_ms, len(read_response.content), f'{store_id}-miss')
                    
                    if (i + 1) % progress_interval == 0:
                        print_warning(f"  [{i+1}/{num_downloads}] Cache MISS, read from {store_id}: {latency_ms:.1f}ms")
                
            except Exception as e:
                metrics.add_failure()
        
        metrics.end()
        
        cache_hit_rate = (cache_hits / num_downloads * 100) if num_downloads > 0 else 0
        
        print(f"\n{Colors.BOLD}Cache Statistics:{Colors.ENDC}")
        print(f"  • Cache hits: {cache_hits}/{num_downloads} ({cache_hit_rate:.1f}%)")
        print(f"  • Cache misses: {cache_misses}/{num_downloads} ({(100-cache_hit_rate):.1f}%)")
        
        if cache_hit_rate < 50:
            print_warning("⚠ Low cache hit rate detected!")
            print_info("  Possible causes:")
            print_info("    - Cache service not properly configured")
            print_info("    - Cache TTL too short")
            print_info("    - Cache eviction happening")
        
        metrics.print_summary()
        
        self.metrics['download_warm'] = metrics
        return metrics
    
    def test_concurrent_downloads(self, photo_id: str, num_concurrent: int = 100, total_requests: int = 1000) -> OperationMetrics:
        """Test concurrent download performance"""
        print_header(f"CONCURRENT DOWNLOAD TEST ({total_requests} requests, {num_concurrent} workers)")
        
        print_info(f"Photo ID: {photo_id[:50]}...")
        print_info(f"Concurrent workers: {num_concurrent}")
        print_info(f"Total requests: {total_requests}")
        print()
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        metrics = OperationMetrics("Concurrent Download Operations")
        metrics.start()
        
        def download_once(request_id: int) -> Tuple[bool, float, int, str]:
            download_start = time.time()
            
            try:
                # Locate
                locate_response = requests.get(
                    f"{self.directory_url}/locate/{photo_id}",
                    timeout=10
                )
                
                if locate_response.status_code != 200:
                    return (False, 0, 0, None)
                
                locations = locate_response.json()['locations']
                location = locations[request_id % len(locations)]
                store_url = translate_docker_url(location['store_url'])
                store_id = location['store_id']
                
                # Download
                read_response = requests.get(
                    f"{store_url}/read/{photo_id}",
                    timeout=10
                )
                
                if read_response.status_code != 200:
                    return (False, 0, 0, None)
                
                download_end = time.time()
                latency_ms = (download_end - download_start) * 1000
                
                return (True, latency_ms, len(read_response.content), store_id)
                
            except Exception as e:
                return (False, 0, 0, None)
        
        progress_interval = max(100, total_requests // 10)
        
        with ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            futures = [executor.submit(download_once, i) for i in range(total_requests)]
            
            completed = 0
            for future in as_completed(futures):
                success, latency_ms, bytes_transferred, store_id = future.result()
                
                if success:
                    metrics.add_success(latency_ms, bytes_transferred, store_id)
                else:
                    metrics.add_failure()
                
                completed += 1
                if completed % progress_interval == 0:
                    print_info(f"  Progress: {completed}/{total_requests}")
        
        metrics.end()
        metrics.print_summary()
        
        self.metrics['download_concurrent'] = metrics
        return metrics
    
    def print_comparative_summary(self):
        """Print comparison across all tests"""
        print_header("COMPARATIVE PERFORMANCE SUMMARY")
        
        if not self.metrics:
            print_warning("No metrics collected")
            return
        
        print(f"\n{Colors.BOLD}Operation Comparison:{Colors.ENDC}\n")
        print(f"{'Operation':<30} {'Ops/sec':<12} {'Avg Latency':<15} {'P95 Latency':<15} {'P99 Latency':<15} {'Success Rate':<12}")
        print("─" * 109)
        
        for name, metrics in self.metrics.items():
            ops_per_sec = metrics.get_throughput_ops_per_sec()
            stats = metrics.latency.get_stats()
            total = metrics.success_count + metrics.failure_count
            success_rate = (metrics.success_count / total * 100) if total > 0 else 0
            
            avg_latency = f"{stats.get('mean', 0):.2f}ms" if stats else "N/A"
            p95_latency = f"{stats.get('p95', 0):.2f}ms" if stats else "N/A"
            p99_latency = f"{stats.get('p99', 0):.2f}ms" if stats else "N/A"
            
            print(f"{name:<30} {ops_per_sec:<12.2f} {avg_latency:<15} {p95_latency:<15} {p99_latency:<15} {success_rate:<11.1f}%")
        
        print()
        
        # Additional insights
        print(f"\n{Colors.BOLD}Key Insights:{Colors.ENDC}")
        
        if 'download_warm' in self.metrics and 'cache-hit' in self.metrics['download_warm'].store_distribution:
            cache_hits = self.metrics['download_warm'].store_distribution.get('cache-hit', 0)
            total_warm = self.metrics['download_warm'].success_count
            hit_rate = (cache_hits / total_warm * 100) if total_warm > 0 else 0
            
            if hit_rate > 90:
                print_success(f"✓ Excellent cache performance: {hit_rate:.1f}% hit rate")
            elif hit_rate > 50:
                print_warning(f"⚠ Moderate cache performance: {hit_rate:.1f}% hit rate")
            else:
                print_error(f"✗ Poor cache performance: {hit_rate:.1f}% hit rate - investigate cache service")
        
        if 'download_cold' in self.metrics and 'download_warm' in self.metrics:
            cold_stats = self.metrics['download_cold'].latency.get_stats()
            warm_stats = self.metrics['download_warm'].latency.get_stats()
            
            if cold_stats and warm_stats:
                speedup = cold_stats['mean'] / warm_stats['mean'] if warm_stats['mean'] > 0 else 1
                print_info(f"ℹ Cache speedup: {speedup:.2f}x faster (warm vs cold)")
        
        print()

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Comprehensive Performance Testing for Haystack System - FIXED VERSION",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all tests with 1000 operations each
  python load_test_fixed.py --all --image image.png --num-operations 1000
  
  # Run only upload test with 500 uploads
  python load_test_fixed.py --upload-test --image image.png --num-uploads 500
  
  # Run download tests with existing photo
  python load_test_fixed.py --download-test --photo-id YOUR_PHOTO_ID --num-downloads 2000
  
  # Run high concurrency test
  python load_test_fixed.py --concurrent-test --photo-id YOUR_PHOTO_ID --concurrent-workers 200 --concurrent-requests 5000
        """
    )
    
    parser.add_argument('--upload-test', action='store_true', help='Run upload performance test')
    parser.add_argument('--download-test', action='store_true', help='Run download performance test')
    parser.add_argument('--concurrent-test', action='store_true', help='Run concurrent download test')
    parser.add_argument('--all', action='store_true', help='Run all tests')
    
    parser.add_argument('--image', type=str, help='Path to image file for upload test')
    parser.add_argument('--photo-id', type=str, help='Photo ID for download tests')
    
    parser.add_argument('--num-uploads', type=int, default=100, help='Number of uploads (default: 100)')
    parser.add_argument('--num-downloads', type=int, default=1000, help='Number of downloads (default: 1000)')
    parser.add_argument('--num-operations', type=int, help='Set all operation counts to this value')
    
    parser.add_argument('--concurrent-workers', type=int, default=100, help='Concurrent workers (default: 100)')
    parser.add_argument('--concurrent-requests', type=int, default=1000, help='Total concurrent requests (default: 1000)')
    
    args = parser.parse_args()
    
    # Override individual counts if --num-operations is set
    if args.num_operations:
        args.num_uploads = args.num_operations
        args.num_downloads = args.num_operations
        args.concurrent_requests = args.num_operations
    
    tester = PerformanceTester()
    
    # Determine what to run
    run_upload = args.upload_test or args.all
    run_download = args.download_test or args.all
    run_concurrent = args.concurrent_test or args.all
    
    # Upload test
    if run_upload:
        if not args.image:
            print_error("--image required for upload test")
            sys.exit(1)
        tester.test_upload_performance(args.image, args.num_uploads)
        # Use first uploaded photo for download tests if no photo_id provided
        if not args.photo_id and hasattr(tester, 'uploaded_photo_ids') and tester.uploaded_photo_ids:
            args.photo_id = tester.uploaded_photo_ids[0]
            print_info(f"\nUsing uploaded photo for download tests: {args.photo_id}")
    
    # Download tests
    if run_download or run_concurrent:
        if not args.photo_id:
            print_error("--photo-id required for download tests (or run upload test first)")
            sys.exit(1)
    
    if run_download:
        tester.test_download_performance_cache_cold(args.photo_id, args.num_downloads)
        
        # Run warm cache test immediately after cold test
        tester.test_download_performance_cache_warm(args.photo_id, args.num_downloads)

    # Concurrent download test
    if run_concurrent:
        tester.test_concurrent_downloads(
            args.photo_id, 
            num_concurrent=args.concurrent_workers, 
            total_requests=args.concurrent_requests
        )

    # Print final comparison of all run tests
    tester.print_comparative_summary()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{Colors.RED}Test interrupted by user{Colors.ENDC}")
        sys.exit(130)
    except Exception as e:
        print(f"\n{Colors.RED}Unexpected error: {e}{Colors.ENDC}")
        sys.exit(1)