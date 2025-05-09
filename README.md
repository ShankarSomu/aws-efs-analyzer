# AWS EFS Analyzer

A Python tool for analyzing Amazon EFS (Elastic File System) mount points to identify cost optimization opportunities.

**IMPORTANT: This tool performs READ-ONLY operations and does not modify any files or change your EFS configuration.**

## Features

- **Recursive Scanning**: Efficiently scans EFS mount points with parallel processing
- **Access Time Analysis**: Categorizes files based on last access time (7, 14, 30, 60, 90 days, 1-2 years, 2+ years)
- **Cost Optimization**: Calculates potential savings across different EFS storage tiers
- **Comprehensive Reports**: Generates detailed HTML and text reports with visualizations
- **Performance Optimized**: Handles large file systems with parallel processing
- **Progress Tracking**: Shows real-time progress with completion percentage and ETA

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

Basic usage:

```bash
python efs_analyzer.py /path/to/efs/mount
```

For more options:

```bash
python efs_analyzer.py --help
```

For usage examples:

```bash
python efs_analyzer.py --examples
```

For root-level directories (requires sudo/admin):

```bash
sudo python efs_analyzer.py / --skip-estimate
```

### Command Line Options

```
usage: efs_analyzer.py [-h] [--parallel PARALLEL] [--exclude [EXCLUDE ...]]
                      [--max-depth MAX_DEPTH] [--follow-symlinks]
                      [--output-dir OUTPUT_DIR] [--log-file LOG_FILE]
                      [--skip-estimate] [--examples] [--yes]
                      mount_point

positional arguments:
  mount_point           EFS mount point to analyze

optional arguments:
  -h, --help            show this help message and exit
  --parallel PARALLEL   Number of parallel processes to use (default: number of CPU cores)
  --exclude [EXCLUDE ...]
                        Directories to exclude from analysis (default: ['proc', 'sys', 'dev', 'run', 'tmp', 'mnt', 'media'])
  --max-depth MAX_DEPTH
                        Maximum directory depth to scan (default: 100)
  --follow-symlinks     Follow symbolic links (use with caution) (default: False)
  --output-dir OUTPUT_DIR
                        Directory to store reports (default: ./reports)
  --log-file LOG_FILE   Log file for errors and warnings (default: efs_analyzer.log)
  --skip-estimate       Skip initial file count estimation (faster start) (default: False)
  --examples            Show usage examples and exit (default: False)
  --yes, -y             Skip confirmation prompt and proceed with scan (default: False)
```

## How It Works

The EFS Analyzer works by:

1. Recursively scanning the specified EFS mount point
2. Categorizing files based on last access time
3. Calculating storage costs across different EFS tiers:
   - **Standard**: $0.30 per GB-month
   - **Infrequent Access (IA)**: $0.025 per GB-month
   - **Archive**: $0.016 per GB-month
4. Generating recommendations for tier transitions
5. Creating detailed reports showing potential savings

## Report Examples

The tool generates two types of reports:

1. **Text Report**: Plain text summary with statistics and recommendations
2. **HTML Report**: Interactive report with visualizations and detailed analysis

## Storage Tier Recommendations

The analyzer uses the following logic for tier recommendations:

- **Standard Tier**: Files accessed within the last 7 days
- **Infrequent Access Tier**: Files accessed between 8-30 days ago
- **Archive Tier**: Files not accessed for more than 30 days

## Performance Considerations

- The tool uses parallel processing which may temporarily increase CPU usage
- On production servers, consider running during non-peak hours
- Use the `--parallel` option to limit the number of processes if needed
- For very large filesystems, use the `--skip-estimate` option for faster startup

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This tool provides estimates based on current AWS pricing. Actual savings may vary based on your specific AWS region, pricing tier, and usage patterns. Always verify recommendations with AWS pricing calculator before making changes to your storage configuration.