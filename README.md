# AWS EFS Analyzer

A Python utility to analyze Amazon EFS (Elastic File System) mounts for cost optimization opportunities.

## Overview

This tool scans an EFS mount point, categorizes files based on last access time, and calculates potential cost savings by moving data between different EFS storage tiers:

- EFS Standard
- EFS Infrequent Access (IA)
- EFS Archive

The analyzer generates reports in both HTML and plain text formats to help you identify cost-saving opportunities and justify storage optimization projects.

## Features

- Scans EFS mount points and analyzes file access patterns
- Categorizes files based on last access time (7, 14, 30, 60, 90 days, 1 year, 2 years, and older)
- Calculates storage size for each access category
- Estimates costs across different EFS storage tiers
- Generates detailed reports in HTML and plain text formats
- Provides recommendations for optimizing storage costs

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/aws-efs-analyzer.git
   cd aws-efs-analyzer
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

Run the script with the path to your EFS mount point:

```
python efs_analyzer.py /path/to/efs/mount
```

### Options

- `--output-dir`, `-o`: Directory to save reports (default: current directory)
- `--exclude`, `-e`: Directories to exclude from analysis
- `--verbose`, `-v`: Enable verbose logging

### Example

```
python efs_analyzer.py /mnt/efs --output-dir ./reports --exclude tmp cache logs --verbose
```

## Reports

The analyzer generates two types of reports:

1. **Plain Text Report**: A simple text-based report with file statistics and cost analysis
2. **HTML Report**: An interactive report with tables and charts for better visualization

Reports are saved in the specified output directory with timestamps in their filenames.

## Cost Optimization Strategy

The analyzer recommends the following storage tier strategy:

- **Standard Tier**: Files accessed within the last 30 days
- **Infrequent Access Tier**: Files accessed between 30-90 days ago
- **Archive Tier**: Files not accessed in over 90 days

## License

This project is licensed under the MIT License - see the LICENSE file for details.