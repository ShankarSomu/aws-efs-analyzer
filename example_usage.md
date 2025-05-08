# EFS Analyzer Usage Examples

## Basic Usage

Analyze an EFS mount point:

```bash
python efs_analyzer.py /mnt/efs
```

## Advanced Options

### Parallel Processing

Control the number of parallel processes (default is number of CPU cores):

```bash
python efs_analyzer.py /mnt/efs --parallel 8
```

### Exclude Directories

Exclude specific directories from analysis:

```bash
python efs_analyzer.py /mnt/efs --exclude cache temp logs
```

### Control Scan Depth

Limit the maximum directory depth to scan:

```bash
python efs_analyzer.py /mnt/efs --max-depth 10
```

### Follow Symbolic Links

Follow symbolic links (use with caution to avoid loops):

```bash
python efs_analyzer.py /mnt/efs --follow-symlinks
```

### Custom Output Directory

Specify where to save the reports:

```bash
python efs_analyzer.py /mnt/efs --output-dir /path/to/reports
```

### Custom Log File

Specify a custom log file for errors and warnings:

```bash
python efs_analyzer.py /mnt/efs --log-file /path/to/custom.log
```

## Example Workflow

1. Run a basic scan to analyze your EFS mount point:

```bash
python efs_analyzer.py /mnt/efs
```

2. Review the generated reports in the `./reports` directory:
   - Text report: `efs_report_YYYYMMDD_HHMMSS.txt`
   - HTML report: `efs_report_YYYYMMDD_HHMMSS.html`

3. Based on the recommendations, implement lifecycle policies in the AWS Console or using AWS CLI/SDK to move data between storage tiers.

## Windows Usage

For Windows systems, specify the mount point as a drive letter or UNC path:

```bash
python efs_analyzer.py E:\
```

or

```bash
python efs_analyzer.py \\server\share
```