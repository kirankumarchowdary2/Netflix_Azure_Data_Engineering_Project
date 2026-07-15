# Netflix Batch Processing Project

<div align="center">

![Netflix](https://img.shields.io/badge/Netflix-E50914?style=for-the-badge&logo=netflix&logoColor=white)
![Azure](https://img.shields.io/badge/Microsoft%20Azure-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

**A scalable, enterprise-grade batch processing pipeline for Netflix data transformation and analytics**

[Overview](#overview) • [Architecture](#architecture) • [Features](#features) • [Setup](#setup) • [Usage](#usage)

</div>

---

## Overview

This project implements a robust batch processing pipeline designed to handle Netflix data at scale using Azure Databricks and Azure Data Factory. The solution leverages modern cloud technologies to provide automated data ingestion, transformation, and analytics capabilities with Delta Live Tables for data quality and streaming support.

### Key Objectives
- ✅ Automated batch data processing from Netflix sources
- ✅ Incremental data loading using Azure Autoloader
- ✅ Data transformation and cleansing using PySpark
- ✅ Real-time data quality validation
- ✅ Scalable architecture with minimal manual intervention

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     DATA SOURCES                         │
│  (GitHub, API Endpoints, External Data Streams)        │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│          AZURE DATA LAKE STORAGE (ADLS)                 │
│  ┌─────────────────────────────────────────────────────┐
│  │  Raw Data Containers: Bronze, Silver, Gold          │
│  └─────────────────────────────────────────────────────┘
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│        AZURE DATABRICKS PROCESSING LAYER                │
│  ┌───────────────────────────────────────────────────┐  │
│  │  Autoloader    │  PySpark    │  Delta Live Tables│  │
│  │  (Incremental) │  (Transform)│  (Quality Checks) │  │
│  └───────────────────────────────────────────────────┘  │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│        AZURE DATA FACTORY ORCHESTRATION                 │
│  ┌───────────────────────────────────────────────────┐  │
│  │  Pipelines • Activities • Linked Services         │  │
│  │  Copy Data • Web Activities • Validation Logic    │  │
│  └───────────────────────────────────────────────────┘  │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│           DATA WAREHOUSE & ANALYTICS                    │
│  (Materialized Views, Streaming Tables, Reports)       │
└─────────────────────────────────────────────────────────┘
```

---

## Features

### 🔄 **Data Ingestion**
- **Azure Autoloader**: Automatically detects and processes new files in ADLS
- **Directory Listing**: Monitors ADLS containers for incremental file changes
- **Incremental Loading**: Only new/modified data is processed, minimizing costs

### 🔀 **Data Transformation**
- **PySpark-based ETL**: Distributed processing for large datasets
- **Dynamic Pipelines**: Parameterized source and target folders for flexibility
- **Data Quality Checks**: Validation activities ensure data integrity
- **Schema Evolution**: Handles changing data structures gracefully

### 📊 **Delta Live Tables (DLT)**
- **Streaming Tables**: Real-time data ingestion and processing
- **Materialized Views**: Pre-computed aggregations for fast queries
- **Data Expectations**: Built-in quality checks with automatic failure handling
- **Automatic Maintenance**: Task orchestration, cluster management, and monitoring

### 🔐 **Security & Access Control**
- **Access Connector**: Secure authentication to Azure resources
- **Resource Groups**: Isolated environments for different stages
- **Linked Services**: Encrypted connections to external systems (GitHub, APIs)

### 📈 **Monitoring & Error Handling**
- **Task Orchestration**: Manages dependencies and execution order
- **Error Handling**: Custom logging and exception management
- **Cost Management**: Tracks and optimizes resource utilization
- **Data Lineage**: Full audit trail of data transformations

---

## Technologies Used

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Cloud Platform** | Microsoft Azure | Infrastructure and managed services |
| **Data Processing** | Azure Databricks | Distributed computing and transformations |
| **Data Storage** | Azure Data Lake Storage (ADLS) | Scalable data repository |
| **Orchestration** | Azure Data Factory | Workflow automation and scheduling |
| **Analytics** | Delta Live Tables | Data quality and streaming analytics |
| **Programming** | Python / PySpark | Data transformation logic |
| **Version Control** | GitHub | Source code management |

---

## Project Structure

```
netflix-batch-processing/
├── README.md                          # Project documentation
├── LICENSE                            # License information
├── .gitignore                         # Git ignore rules
│
├── notebooks/
│   ├── 01_data_ingestion.py          # Autoloader setup & incremental loading
│   ├── 02_data_transformation.py     # PySpark transformation logic
│   ├── 03_dlt_pipeline.py            # Delta Live Tables definitions
│   └── 04_validation_activity.py     # Data quality checks
│
├── scripts/
│   ├── setup_infrastructure.py       # Azure resource provisioning
│   ├── configure_adls.py             # ADLS container and access setup
│   └── deploy_pipelines.py           # ADF pipeline deployment
│
├── pipelines/
│   ├── main_pipeline.json            # Primary orchestration pipeline
│   ├── activities/
│   │   ├── copy_data_activity.json   # Data copy definitions
│   │   ├── web_activity.json         # API integrations
│   │   └── validation_activity.json  # Quality validation
│   └── linked_services/
│       ├── adls_connection.json      # Data Lake connection
│       ├── github_service.json       # GitHub integration
│       └── databricks_service.json   # Databricks workspace
│
├── config/
│   ├── environment.yml               # Environment configuration
│   ├── parameters.json               # Pipeline parameters
│   └── data_expectations.yaml        # DLT quality rules
│
└── docs/
    ├── ARCHITECTURE.md               # Detailed architecture guide
    ├── SETUP_GUIDE.md                # Step-by-step setup instructions
    ├── TROUBLESHOOTING.md            # Common issues and solutions
    └── API_REFERENCE.md              # Function and activity documentation
```

---

## Prerequisites

Before getting started, ensure you have:

- ✅ **Azure Subscription** with active billing
- ✅ **Azure Databricks Workspace** (Standard or Premium tier)
- ✅ **Azure Data Lake Storage Gen2** account created
- ✅ **Azure Data Factory** instance
- ✅ **Python 3.8+** installed locally
- ✅ **Git** and **GitHub** account for version control
- ✅ **Azure CLI** installed and configured
- ✅ **Databricks CLI** for notebook management

---

## Setup & Installation

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/netflix-batch-processing.git
cd netflix-batch-processing
```

### 2. Azure Infrastructure Setup

#### Create Azure Resources
```bash
# Login to Azure
az login

# Create Resource Group
az group create --name netflix-rg --location eastus

# Create Storage Account
az storage account create \
  --name netflixdatalake \
  --resource-group netflix-rg \
  --location eastus \
  --sku Standard_LRS
```

#### Create ADLS Containers
```bash
# Create containers for data lake layers
az storage container create --name bronze --account-name netflixdatalake
az storage container create --name silver --account-name netflixdatalake
az storage container create --name gold --account-name netflixdatalake
```

### 3. Configure Access Connector

```bash
# Create Managed Identity for Access Connector
az identity create --name netflix-connector --resource-group netflix-rg

# Assign Storage Blob Data Contributor role
az role assignment create \
  --assignee-object-id <MANAGED-IDENTITY-ID> \
  --role "Storage Blob Data Contributor" \
  --scope /subscriptions/<SUBSCRIPTION-ID>/resourceGroups/netflix-rg/providers/Microsoft.Storage/storageAccounts/netflixdatalake
```

### 4. Databricks Configuration

#### Create Databricks Workspace
```bash
# Create Databricks workspace
az databricks workspace create \
  --name netflix-workspace \
  --resource-group netflix-rg \
  --location eastus \
  --sku premium
```

#### Configure Workspace Libraries
```bash
# Install required Python packages
pip install databricks-sdk pyspark azure-storage-file-datalake pyyaml
```

#### Import Notebooks
Upload all notebooks from `notebooks/` directory to your Databricks workspace.

### 5. Azure Data Factory Setup

#### Deploy Linked Services
```bash
# Create ADLS linked service
az datafactory linked-service create \
  --factory-name netflix-adf \
  --resource-group netflix-rg \
  --name "ADLSLinkedService" \
  --properties @pipelines/linked_services/adls_connection.json
```

#### Create Pipelines
```bash
# Deploy main pipeline
az datafactory pipeline create \
  --factory-name netflix-adf \
  --resource-group netflix-rg \
  --name "MainBatchPipeline" \
  --pipeline @pipelines/main_pipeline.json
```

### 6. Environment Configuration

Create `.env` file in project root:
```env
# Azure Configuration
AZURE_SUBSCRIPTION_ID=your_subscription_id
AZURE_RESOURCE_GROUP=netflix-rg
AZURE_STORAGE_ACCOUNT=netflixdatalake

# Databricks Configuration
DATABRICKS_HOST=https://your-databricks-instance.cloud.databricks.com
DATABRICKS_TOKEN=your_databricks_token

# Data Lake Configuration
BRONZE_CONTAINER=bronze
SILVER_CONTAINER=silver
GOLD_CONTAINER=gold

# Data Factory Configuration
DATAFACTORY_NAME=netflix-adf
PIPELINE_NAME=MainBatchPipeline
```

---

## Usage

### Running the Pipeline Manually

#### Option 1: Trigger via Azure Portal
1. Navigate to Azure Data Factory
2. Open the "MainBatchPipeline"
3. Click "Add Trigger" → "Trigger Now"
4. Monitor execution in Pipeline Runs

#### Option 2: Trigger via Azure CLI
```bash
az datafactory pipeline create-run \
  --factory-name netflix-adf \
  --resource-group netflix-rg \
  --name MainBatchPipeline
```

#### Option 3: Trigger via Python SDK
```python
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

credential = DefaultAzureCredential()
client = DataFactoryManagementClient(credential, subscription_id)

run = client.pipelines.create_run(
    resource_group_name="netflix-rg",
    factory_name="netflix-adf",
    pipeline_name="MainBatchPipeline"
)

print(f"Pipeline run ID: {run.run_id}")
```

### Running Databricks Notebooks

#### Execute Transformation Notebook
```python
# In Databricks workspace
%run ./02_data_transformation

# Check transformation results
display(spark.read.format("delta").load("/mnt/silver/netflix_data"))
```

#### Monitor DLT Pipeline
```bash
# List DLT pipelines
databricks pipelines list

# Monitor specific pipeline
databricks pipelines get-update <PIPELINE_ID>
```

### Scheduling Automated Runs

#### Create Scheduled Trigger
```bash
az datafactory trigger create \
  --factory-name netflix-adf \
  --resource-group netflix-rg \
  --name "DailyBatchTrigger" \
  --trigger-type ScheduleTrigger \
  --frequency Daily \
  --interval 1 \
  --start-time "2024-01-01T00:00:00Z"
```

---

## Configuration & Customization

### Modifying Pipeline Parameters

Edit `config/parameters.json`:
```json
{
  "source_folder": "/bronze/raw_data",
  "target_folder": "/silver/processed_data",
  "batch_size": 1000,
  "retry_count": 3,
  "error_threshold": 0.05
}
```

### Customizing Data Expectations

Edit `config/data_expectations.yaml`:
```yaml
tables:
  netflix_raw:
    expectations:
      - column: title
        expectation: not_null
        action: drop_row
      - column: rating
        expectation: between(0, 10)
        action: log_error
```

---

## Monitoring & Troubleshooting

### View Pipeline Logs
```bash
# Get last 10 pipeline runs
az datafactory pipeline list-runs \
  --factory-name netflix-adf \
  --resource-group netflix-rg \
  --last-updated-after "2024-01-01T00:00:00Z"
```

### Debug Databricks Jobs
```bash
# Monitor Databricks job runs
databricks jobs list-runs --job-id <JOB_ID>

# Get detailed run information
databricks runs get --run-id <RUN_ID>
```

### Check ADLS Connectivity
```bash
# Test access to ADLS
az storage blob list --container-name bronze --account-name netflixdatalake
```

---

## Performance Optimization

### Tips for Faster Processing
- **Partition Data**: Organize data by date/region for parallel processing
- **Optimize Cluster Size**: Scale compute resources based on data volume
- **Use Caching**: Cache frequently accessed datasets
- **Enable Z-ordering**: Organize data for faster queries
- **Monitor Costs**: Track resource utilization and optimize accordingly

### Recommended Cluster Configuration
```json
{
  "spark_version": "11.3.x-scala2.12",
  "node_type_id": "i3.xlarge",
  "num_workers": 4,
  "autoscale": {
    "min_workers": 2,
    "max_workers": 8
  },
  "aws_attributes": {
    "availability": "SPOT_WITH_FALLBACK"
  }
}
```

---

## Contributing

We welcome contributions! Please follow these steps:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/AmazingFeature`)
3. **Commit** your changes (`git commit -m 'Add AmazingFeature'`)
4. **Push** to the branch (`git push origin feature/AmazingFeature`)
5. **Open** a Pull Request

### Code Standards
- Use Python PEP 8 for code formatting
- Add docstrings to all functions
- Include unit tests for new features
- Update documentation as needed

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Support & Resources

### Documentation
- [Azure Databricks Documentation](https://docs.databricks.com/)
- [Azure Data Factory Guide](https://learn.microsoft.com/en-us/azure/data-factory/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)

### Useful Links
- [Azure CLI Documentation](https://learn.microsoft.com/en-us/cli/azure/)
- [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/)

### Getting Help
- 📧 Email: support@example.com
- 💬 GitHub Issues: [Create an issue](https://github.com/yourusername/netflix-batch-processing/issues)
- 📚 Wiki: [Project Wiki](https://github.com/yourusername/netflix-batch-processing/wiki)

---

## Roadmap

- [ ] Add real-time streaming support via Apache Kafka
- [ ] Implement advanced ML model integration
- [ ] Create interactive dashboards with Power BI
- [ ] Add cost optimization recommendations
- [ ] Support for multi-cloud deployment
- [ ] Enhanced data lineage and impact analysis
- [ ] Advanced anomaly detection capabilities

---

## Acknowledgments

- Netflix for the inspiration
- Azure Databricks team for excellent documentation
- Community contributors and supporters

---

<div align="center">

**Made with ❤️ by [Your Name/Team]**

⭐ If this project helped you, please consider giving it a star! ⭐

</div>
