# AWS EFS Analyzer

A Python tool for analyzing Amazon EFS (Elastic File System) mount points to identify cost optimization opportunities.

## Features

- **Recursive Scanning**: Efficiently scans EFS mount points with parallel processing
- **Access Time Analysis**: Categorizes files based on last access time
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

See [example_usage.md](example_usage.md) for detailed usage examples.

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

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This tool provides estimates based on current AWS pricing. Actual savings may vary based on your specific AWS region, pricing tier, and usage patterns. Always verify recommendations with AWS pricing calculator before making changes to your storage configuration.