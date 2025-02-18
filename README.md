# pbf_datasource 🗺️

A Spark-based data processing pipeline for handling OpenStreetMap PBF files, providing efficient geospatial data conversion and filtering capabilities.

## Features

- **Custom Spark Datasource** for OSM PBF files
- Multiple geometry output formats (WKT, WKB, GeoJSON)
- Advanced filtering capabilities:
  - Empty tag filtering
  - Key-based filtering
  - Specific tag-value filtering
- Databricks integration with pre-configured workflows
- Performance benchmarking suite

## Installation

### Prerequisites
- Python 3.8+
- PySpark 3.3+
- osmium-tool
- Databricks CLI (for deployment)

```
%pip install osmium
```

### Deployment
1. Configure Databricks CLI:
```
databricks configure
```
2. Deploy to workspace:
```
databricks bundle deploy --target dev # Development
databricks bundle deploy --target prod # Production
```

## Usage

### Basic Example
```
df = (spark.read.format("pbf")
.option("path", "/path/to/file.osm.pbf")
.option("geometryType", "WKT")
.load())
```

### Advanced Filtering
```
df = (spark.read.format("pbf")
.option("path", "/path/to/file.osm.pbf")
.option("emptyTagFilter", True)
.option("keyFilter", "building")
.option("tagFilter", "('building', 'hospital')")
.load())
```

## Project Structure

```
├── src/
│ ├── benchmark.ipynb # Performance testing suite
│ ├── demo_notebook.ipynb # Usage examples
│ ├── download_pbf.ipynb # Geofabrik data downloader
│ ├── load_datasource.ipynb # Core conversion logic
│ └── pbf_ingest.ipynb # Production ingestion pipeline
├── resources/ # Databricks job configurations
└── databricks.yml # Bundle configuration
```


## Performance Benchmarks

| Region          | File Size | Processing Time |
|-----------------|-----------|-----------------|
| Malta           | 6.95 MB   | 6.95 seconds    |
| France          | ~4GB      | 42.39 minutes   |
| North America   | ~20GB     | 1.25 hours      |

**Tip:** Filter early (`emptyTagFilter` → `keyFilter` → `tagFilter`) for optimal performance.


## Acknowledgements
- Geofabrik for OSM data mirrors
- Databricks for Spark integration
- Osmium contributors for core parsing logic

