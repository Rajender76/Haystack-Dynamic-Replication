# Haystack Distributed Storage System

Implemented the distributed photo storage system inspired by Facebook’s Haystack, enhanced with dynamic replication, strong consistency and different distributed concepts.

### System Architecture

```
                    ┌─────────────┐
                    │   Client    │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  Nginx LB   │  ← Load Balancer
                    └──────┬──────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
    ┌────▼────┐       ┌────▼────┐      ┌────▼────┐
    │Directory│       │Directory│      │Directory│  ← Leader Election
    │   -1    │       │   -2    │      │   -3    │     (Redis-based)
    └────┬────┘       └────┬────┘      └────┬────┘
         │                 │                 │
         └─────────────────┼─────────────────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
    ┌────▼────┐       ┌────▼────┐      ┌────▼────┐
    │ Store-1 │       │ Store-2 │      │ Store-3 │  ← Needle Storage
    └─────────┘       └─────────┘      └─────────┘     (5 stores)
         │                 │                 │
         └─────────────────┼─────────────────┘
                           │
                      ┌────▼────┐
                      │  Redis  │  ← Cache + Elections
                      └─────────┘
                           │
                      ┌────▼─────────┐
                      │ Replication  │  ← Smart Replication
                      │   Manager    │     & Monitoring
                      └──────────────┘
```



### CAP Theorem Choice – CP System
## Why CP?

All metadata writes go through a single elected leader.

Followers forward writes to the leader to ensure consistent metadata.

Stores continue serving reads even during network partitions.

## Tradeoff

Writes stall for ~10–15s during leader failover.


## Implemented Improvements

#### 1. Tombstone Deletes 


#### 2. Redis-Based Leader Election

#### 3. Health-Aware Location Filtering 
**Problem:** Directory returned DOWN stores as valid locations.  

#### 4.  Nightly Full Audit
**Problem:** Cold data never checked for under-replication.  

#### 5. Garbage Collection

### High Priority Improvements (Performance & Consistency)

#### 6. Redis Cache

#### 7. Nginx Load Balancer

#### 8. Push Notifications

###  Medium Priority Improvements (Efficiency & Scale)

#### 9. Push-on-Write Caching
**Problem:** Client uploaded to cache, doubling network traffic.  

#### 11.  Dynamic Store Discovery

#### 13. De-replication
**Problem:** Over-replicated photos wasted space.  
**Solution:** Removes excess replicas from stores with least capacity.  

#### 16. Checksums
**Problem:** No data integrity verification.  
**Solution:** SHA256 checksums stored in directory metadata.  
**Impact:** Detect corruption, future verification support.


##  Quick Start

### Prerequisites
- Docker & Docker Compose
- 20GB+ free disk space
- 8GB+ RAM recommended

### Setup

1. **Clone and navigate:**
```bash
git clone <repo>
cd haystack-distributed-storage
```

2. **Start the system:**
```bash
docker-compose up -d
```

3. **Wait for initialization (~30 seconds):**
```bash
# Watch logs
docker-compose logs -f

# Check status
docker exec haystack-client python check_status.py
```

4. **Upload a photo:**
```bash
docker exec -it haystack-client bash
python client_service.py upload /images/photo.jpg
```

## Usage Examples

### Upload Photo
```bash
docker exec haystack-client python client_service.py upload /images/photo.jpg
# Output: Photo ID: a3f5c8b9d2e1...
```

### Download Photo
```bash
docker exec haystack-client python client_service.py download <photo_id> /tmp/downloaded.jpg
```

### Check Photo Status
```bash
docker exec haystack-client python client_service.py status <photo_id>
```

### System Statistics
```bash
docker exec haystack-client python client_service.py stats
```

### Health Check
```bash
docker exec haystack-client python client_service.py health
```

### Full Status Report
```bash
docker exec haystack-client python check_status.py
```

## Configuration

### Environment Variables

**Store Service:**
```env
MAX_VOLUME_SIZE=4294967296  # 4GB per volume
COMPACTION_EFFICIENCY_THRESHOLD=0.60  # Compact at 60%
HEARTBEAT_INTERVAL=30  # Heartbeat every 30s
STATS_REPORT_INTERVAL=60  # Report stats every 60s
```

**Directory Service:**
```env
LEADER_TIMEOUT=10  # Leader election timeout
LEADER_HEARTBEAT_INTERVAL=3  # Leader refresh interval
FOLLOWER_SYNC_INTERVAL=0.5  # Fast follower sync
```

**Replication Manager:**
```env
DEFAULT_REPLICA_COUNT=3  # Target replicas
MIN_REPLICA_COUNT=2  # Minimum replicas
MAX_REPLICA_COUNT=5  # Maximum replicas
ACCESS_RATE_THRESHOLD=200  # Hot photo threshold (req/min)
NIGHTLY_AUDIT_HOUR=2  # Run full audit at 2 AM
```

**Cache Service:**
```env
CACHE_TTL=3600  # 1 hour TTL
```

### Monitoring Commands

```bash
docker-compose logs -f

docker-compose logs -f replication

# Check Redis
docker exec haystack-redis redis-cli INFO memory

# Check system status
docker exec haystack-client python check_status.py
```

## Architecture Details

### Data Flow: Upload
```
Client → Nginx → Directory Leader → Allocate Store-3
Client → Store-3 → Write to volume + Push to Redis
Client → Directory Leader → Register location
Directory Leader → Push notification → Followers
Directory → Replication Manager → Schedule replication tasks
Workers → Replicate to Store-1, Store-4
```

### Data Flow: Download
```
Client → Redis Cache → Cache Hit → Return photo (fast path)
Client → Redis Cache → Cache Miss → Query Directory
Directory → Filter healthy stores → Return locations
Client → Store-1 → Read from volume → Return photo
```

### Leader Election Flow
```
All directory instances → Redis SET NX "leader" with 10s TTL
Winner → Becomes leader, handles writes
Leader → Refresh TTL every 3 seconds
Leader dies → TTL expires after 10s
New leader → Claims leadership via SET NX
Followers → Sync from new leader via push + poll
```

### Replication Decision Flow
```
Store → Report access stats every 60s → Replication Manager
Replication Manager → Analyze access patterns
  - Cold photo, < min replicas → URGENT priority
  - Hot photo (>500 req/min) → Increase to 5 replicas
  - Excess replicas → De-replicate from full stores
Replication Manager → Create tasks in priority queue
Workers → Execute replications in parallel
Nightly (2 AM) → Full audit of ALL photos
```


### Team Members
SRIRAM METLA - 2024201059
ANNAM RAJENDER REDDY - 2024202016
ADHIVISHNU MODAPU - 2024202028

