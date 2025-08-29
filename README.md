# Data Engineering Pipelines

A collection of modern data engineering pipelines built with Databricks, implementing various architectural patterns and use cases.

## 🏗️ Pipelines

### Telco CDR Processing Platform
**Location**: [`dlt_telco/`](dlt_telco/)

A scalable telecommunications Call Detail Records (CDR) processing platform using Delta Live Tables and the medallion architecture.

- **Architecture**: Bronze → Silver → Gold layers
- **Technology**: Databricks Delta Live Tables, Kafka, Unity Catalog
- **Use Case**: Real-time telco data processing and analytics
- **Status**: Bronze layer complete, Silver/Gold in development

[📖 Full Documentation](dlt_telco/README.md)

### DLT 101 Pipeline
**Location**: [`dlt_101/`](dlt_101/)

Basic Delta Live Tables pipeline for learning and experimentation.

- **Architecture**: Streaming bronze layer
- **Technology**: Databricks Delta Live Tables
- **Use Case**: Educational and prototyping
- **Status**: Active

## 🛠️ Shared Components

### Data Generators
**Location**: [`src/cdr/data_generator/`](src/cdr/data_generator/)

Synthetic data generation utilities for testing and development:
- User profile generator
- CDR generator (Voice, Data, SMS, VoIP, IMS)
- Kafka producer for streaming data

### Common Dependencies
**Location**: [`requirements.txt`](requirements.txt)

Shared Python dependencies across all pipelines.

## 🚀 Quick Start

1. **Prerequisites**:
   - Databricks workspace with Unity Catalog
   - Databricks CLI configured
   - Python 3.8+

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Choose a pipeline** and follow its specific documentation

## 📁 Project Structure

```
data/
├── dlt_telco/                 # Telco CDR processing pipeline
├── dlt_101/                   # Basic DLT learning pipeline
├── src/                       # Shared source code
│   └── cdr/data_generator/    # Data generation utilities
├── requirements.txt           # Shared dependencies
└── README.md                 # This file
```

## 🔄 CI/CD

*Coming Soon*

Planned CI/CD implementation will include:
- Automated testing for all pipelines
- Multi-environment deployment (dev/staging/prod)
- Data quality validation
- Performance monitoring

## 📚 Documentation

- [Telco CDR Pipeline](dlt_telco/README.md)
- [Data Generator Guide](src/cdr/data_generator/README.md)

## 📄 License

This project is licensed under the MIT License.
