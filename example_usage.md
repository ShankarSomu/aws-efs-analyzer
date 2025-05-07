# AWS EFS Analyzer - Example Usage

This document provides examples of how to use the AWS EFS Analyzer tool in different scenarios.

## Basic Usage

Analyze an EFS mount point and generate reports in the current directory:

```bash
python efs_analyzer.py /mnt/efs
```

## Specifying Output Directory

Save reports to a specific directory:

```bash
python efs_analyzer.py /mnt/efs --output-dir /path/to/reports
```

## Excluding Directories

Exclude specific directories from analysis (useful for temporary or cache directories):

```bash
python efs_analyzer.py /mnt/efs --exclude tmp cache .git node_modules
```

## Complete Example

A complete example with all options:

```bash
python efs_analyzer.py /mnt/efs --output-dir ./reports --exclude tmp cache logs .git --verbose
```

## Sample Output

### Text Report Excerpt

```
================================================================================
EFS ANALYZER REPORT
================================================================================
Mount Point: /mnt/efs
Analysis Date: 2023-06-15 14:30:45

FILE ACCESS STATISTICS
--------------------------------------------------------------------------------
Category             Count      Size           Percentage  
--------------------------------------------------------------------------------
last_7_days          1250       2.5 GB         10.00%
last_14_days         2500       5.0 GB         20.00%
last_30_days         3750       7.5 GB         30.00%
last_60_days         2500       5.0 GB         20.00%
last_90_days         1250       2.5 GB         10.00%
last_1_year          625        1.25 GB        5.00%
last_2_years         375        750 MB         3.00%
older                250        500 MB         2.00%

COST ANALYSIS
--------------------------------------------------------------------------------
Total Storage: 25.0 GB
Current Monthly Cost (Standard tier): $7.50
Optimized Monthly Cost: $3.13
Monthly Savings: $4.37 (58.27%)
```

### HTML Report

The HTML report includes the same information as the text report but with additional visualizations and interactive elements.

## Interpreting Results

The reports provide insights into:

1. **File Access Patterns**: How frequently files are being accessed
2. **Storage Distribution**: How much storage is used by files in each access category
3. **Cost Analysis**: Current costs vs. optimized costs using different storage tiers
4. **Recommendations**: Suggestions for optimizing storage costs

## Next Steps After Analysis

1. **Review the Reports**: Understand your file access patterns and storage distribution
2. **Implement Lifecycle Policies**: Configure EFS lifecycle management to automatically move files between storage tiers
3. **Monitor Costs**: After implementing changes, monitor your AWS costs to verify savings
4. **Regular Analysis**: Run the analyzer periodically to identify new optimization opportunities

## Troubleshooting

If you encounter permission issues, ensure that:

1. You have read access to all directories in the EFS mount
2. You're running the script with appropriate permissions (consider using sudo if necessary)

For large file systems, the analysis may take a significant amount of time. Consider running it during off-peak hours or using the `--exclude` option to skip directories that don't need analysis.