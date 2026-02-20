# pub-sub-kiss-icp

A pub-sub wrapper for [KISS-ICP](https://github.com/PRBonn/kiss-icp): subscribe to a point cloud topic, generate LiDAR odometry estimates with KISS-ICP, and publish the results to an outbound topic.

## Supported Protocols

| Protocol | Type key | Extra |
|---|---|---|
| Apache Kafka | `kafka` | `[kafka]` |
| Apache Pulsar | `pulsar` | `[pulsar]` |
| StreamNative Cloud | `streamnative` | `[pulsar]` |
| Azure Event Hubs | `eventhubs` | `[eventhubs]` |
| Iggy | `iggy` | `[iggy]` |
| Google Cloud Pub/Sub | `googlepubsub` | `[googlepubsub]` |
| Mock (in-memory) | `mock` | *(built-in)* |

## Installation

```bash
# Core (no broker clients)
pip install -e .

# With a specific adapter
pip install -e ".[kafka]"
pip install -e ".[pulsar]"
pip install -e ".[eventhubs]"
pip install -e ".[iggy]"
pip install -e ".[googlepubsub]"

# All adapters
pip install -e ".[all]"
```

## Quick Start

### 1. Create a configuration file

See the `examples/` directory for ready-to-use YAML configs.

```yaml
# examples/kafka-config.yaml
source:
  type: kafka
  topic: pointcloud-input
  connection:
    bootstrap.servers: "localhost:9092"
    group.id: kiss-icp-group
    auto.offset.reset: earliest

destination:
  type: kafka
  topic: odometry-output
  connection:
    bootstrap.servers: "localhost:9092"

kiss_icp:
  max_range: 100.0
  voxel_size: 1.0
  deskew: true
```

### 2. Run the pipeline

```bash
pub-sub-kiss-icp -c examples/kafka-config.yaml
# Or with a log level and frame limit:
pub-sub-kiss-icp -c examples/mock-config.yaml -l DEBUG --max-frames 100
```

## Message Formats

### Inbound – Point Cloud

Binary frame with an 8-byte magic header followed by metadata and float32 XYZ(I) data:

```
[8B magic] [4B num_points uint32] [4B point_dim uint32] [8B timestamp float64] [N×dim×4B float32 data]
```

Use the helper functions in `pub_sub_kiss_icp.serialization` to produce compliant messages:

```python
import numpy as np
from pub_sub_kiss_icp.serialization import serialize_pointcloud

points = np.random.rand(1000, 3).astype(np.float32)  # (N, 3) or (N, 4)
payload = serialize_pointcloud(points, timestamp=1_700_000_000.0)
# publish `payload` to your broker
```

### Outbound – Odometry

UTF-8 encoded JSON:

```json
{
  "frame_id": 42,
  "timestamp": 1700000042.0,
  "pose": [[1,0,0,x],[0,1,0,y],[0,0,1,z],[0,0,0,1]]
}
```

`pose` is a 4×4 homogeneous transformation matrix (sensor in world frame).

## Configuration Reference

| Key | Default | Description |
|---|---|---|
| `source.type` | *(required)* | Protocol type (see table above) |
| `source.topic` | *(required)* | Inbound topic/subscription name |
| `source.connection` | `{}` | Broker-specific connection parameters |
| `destination.type` | *(required)* | Protocol type |
| `destination.topic` | *(required)* | Outbound topic name |
| `destination.connection` | `{}` | Broker-specific connection parameters |
| `kiss_icp.max_range` | `100.0` | Maximum point range (metres) |
| `kiss_icp.min_range` | `0.0` | Minimum point range (metres) |
| `kiss_icp.voxel_size` | `null` | Voxel size for down-sampling (`null` = auto) |
| `kiss_icp.deskew` | `true` | Enable motion de-skewing |
| `kiss_icp.initial_threshold` | `2.0` | Adaptive threshold seed value |
| `kiss_icp.min_motion_th` | `0.1` | Minimum motion threshold |
| `poll_timeout_ms` | `1000` | Subscriber poll timeout in milliseconds |

## Python API

```python
from pub_sub_kiss_icp.adapters.mock import MockSubscriber, MockPublisher
from pub_sub_kiss_icp.pipeline import KissICPPipeline
from pub_sub_kiss_icp.serialization import serialize_pointcloud
import numpy as np

sub = MockSubscriber()
pub = MockPublisher()

# Pre-load some frames
for i in range(10):
    points = np.random.rand(500, 3).astype(np.float32)
    sub.push(serialize_pointcloud(points, timestamp=float(i)))

pipeline = KissICPPipeline(subscriber=sub, publisher=pub)
pipeline.run(max_frames=10)

print(f"Processed {pipeline.frames_processed} frames")
print(f"Published {len(pub.messages)} odometry messages")
```

## Architecture

```
┌──────────────────┐     ┌─────────────────────┐     ┌──────────────────┐
│   Subscriber     │────▶│  KissICPProcessor   │────▶│   Publisher      │
│  (any protocol)  │     │  (KISS-ICP + pose)  │     │  (any protocol)  │
└──────────────────┘     └─────────────────────┘     └──────────────────┘
        ▲                                                      ▲
  deserialize_pointcloud()                        serialize_odometry()
```

## Running Tests

```bash
pip install -e ".[dev]"
pytest
```

## License

MIT License
