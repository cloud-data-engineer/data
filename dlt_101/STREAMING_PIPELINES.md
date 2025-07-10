# Streaming DLT Pipelines

This project now includes streaming Delta Live Tables (DLT) pipelines for bronze layer data ingestion.

## Available Pipelines

### 1. Basic Bronze Streaming Pipeline
- **File**: `src/bronze_streaming_pipeline.sql`
- **Config**: `resources/bronze_streaming.pipeline.yml`
- **Purpose**: Simple streaming pipeline that reads JSON files and creates a bronze layer table

### 2. Custom Bronze Streaming Pipeline (Recommended)
- **File**: `src/custom_bronze_streaming.sql`
- **Config**: `resources/custom_bronze_streaming.pipeline.yml`
- **Purpose**: Production-ready streaming pipeline with comprehensive data quality monitoring

## Features

### Bronze Layer Capabilities
- **Streaming Ingestion**: Automatically processes new JSON files as they arrive
- **Data Quality Validation**: Built-in constraints to handle malformed records
- **Metadata Enrichment**: Adds ingestion timestamps and source file tracking
- **Monitoring**: Quality metrics and alerting for data issues

### Data Quality Features
- Malformed record detection and handling
- Data quality percentage tracking
- Alert system for quality issues
- Ingestion metrics and monitoring

## Deployment

### Deploy the streaming pipeline:
```bash
# Deploy to development
databricks bundle deploy --target dev

# Deploy to production
databricks bundle deploy --target prod
```

### Run the pipeline:
```bash
# Run the basic streaming pipeline
databricks bundle run bronze_streaming_pipeline

# Run the custom streaming pipeline
databricks bundle run custom_bronze_streaming_pipeline
```

## Customization

### To adapt for your JSON data source:

1. **Update the source path** in the SQL files:
   ```sql
   FROM STREAM(
     read_files(
       "/your/json/file/path/", -- Update this path
       format => "json",
       header => "false"
     )
   );
   ```

2. **Modify data quality constraints** based on your data schema:
   ```sql
   CONSTRAINT your_constraint EXPECT (your_condition) ON VIOLATION DROP ROW
   ```

3. **Configure pipeline settings** in the YAML files:
   - Adjust compute resources
   - Set up notifications
   - Configure permissions

## Monitoring

The pipelines create several monitoring tables:

- `bronze_data_quality`: Quality metrics per file
- `bronze_ingestion_metrics`: Detailed ingestion statistics
- `bronze_data_quality_alerts`: Automated quality alerts

## Best Practices

1. **Start with the custom pipeline** - it includes comprehensive monitoring
2. **Test with small datasets** before processing large volumes
3. **Monitor data quality metrics** regularly
4. **Set up alerts** for production deployments
5. **Use appropriate compute sizing** based on your data volume

## Troubleshooting

### Common Issues:
- **File path errors**: Ensure the JSON file path is accessible
- **Schema mismatches**: Check JSON structure matches expectations
- **Performance issues**: Adjust cluster size or enable auto-scaling

### Debugging:
- Check pipeline logs in the Databricks UI
- Review data quality metrics tables
- Monitor streaming progress in the pipeline dashboard
