#!/usr/bin/env python3
"""
EFS Analyzer - Analyzes EFS mount points for cost optimization opportunities

This script scans an Amazon EFS (Elastic File System) mount point to identify cost 
optimization opportunities by categorizing files based on last access time and 
calculating potential cost savings across different EFS storage tiers.

Features:
- Recursive scanning with parallel processing for large file systems
- File categorization based on last access time (7, 14, 30, 60, 90 days, 1-2 years, 2+ years)
- Cost analysis across different EFS tiers (Standard, Infrequent Access, Archive)
- Detailed HTML and text reports with visualizations
- Real-time progress tracking
- System directory exclusion and symbolic link loop detection

IMPORTANT: This tool performs READ-ONLY operations and does not modify any files
or change your EFS configuration. It only analyzes file metadata to provide
recommendations for potential cost savings.

Usage:
    python efs_analyzer.py /path/to/efs/mount [options]
    
For root-level directories:
    sudo python efs_analyzer.py / --skip-estimate

For usage examples:
    python efs_analyzer.py --examples
"""

import os
import sys
import time
import argparse
import logging
import platform
import datetime
import humanize
import multiprocessing
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
import json
import jinja2
import matplotlib.pyplot as plt
from tqdm import tqdm

# EFS pricing constants (US East - N. Virginia)
PRICING = {
    'standard': 0.30,  # per GB-month
    'ia': 0.025,       # per GB-month
    'archive': 0.016   # per GB-month
}

# Global variables
logger = None
total_files = 0
processed_files = 0
start_time = None
progress_bar = None

class FileStats:
    """
    Stores and manages file statistics categorized by last access time.
    
    This class maintains counters for file sizes across different access time categories
    and provides methods to add files and merge statistics from multiple scans.
    
    Attributes:
        categories (dict): Dictionary mapping access time categories to total size in bytes
        total_size (int): Total size of all files in bytes
        total_files (int): Total number of files processed
        errors (int): Count of errors encountered during processing
    """
    
    def __init__(self):
        # Initialize categories based on last access time
        self.categories = {
            "0-7_days": 0,
            "8-14_days": 0,
            "15-30_days": 0,
            "31-60_days": 0,
            "61-90_days": 0,
            "91-365_days": 0,
            "1-2_years": 0,
            "2+_years": 0
        }
        self.total_size = 0
        self.total_files = 0
        self.errors = 0

    def add_file(self, size, last_access_days):
        """
        Add a file to the appropriate category based on last access time.
        
        Args:
            size (int): Size of the file in bytes
            last_access_days (int): Number of days since the file was last accessed
            
        Returns:
            None
        """
        self.total_size += size
        self.total_files += 1
        
        if last_access_days <= 7:
            self.categories["0-7_days"] += size
        elif last_access_days <= 14:
            self.categories["8-14_days"] += size
        elif last_access_days <= 30:
            self.categories["15-30_days"] += size
        elif last_access_days <= 60:
            self.categories["31-60_days"] += size
        elif last_access_days <= 90:
            self.categories["61-90_days"] += size
        elif last_access_days <= 365:
            self.categories["91-365_days"] += size
        elif last_access_days <= 730:  # 2 years
            self.categories["1-2_years"] += size
        else:
            self.categories["2+_years"] += size

    def merge(self, other):
        """
        Merge another FileStats object into this one.
        
        This method combines statistics from another FileStats object with this one,
        useful when aggregating results from parallel processing.
        
        Args:
            other (FileStats): Another FileStats object to merge into this one
            
        Returns:
            None
        """
        self.total_size += other.total_size
        self.total_files += other.total_files
        self.errors += other.errors
        
        for category, size in other.categories.items():
            self.categories[category] += size

def setup_logging(log_file):
    """
    Configure logging to write messages to a file.
    
    Sets up a logger that writes INFO level and above messages to the specified log file.
    
    Args:
        log_file (str): Path to the log file
        
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger("efs_analyzer")
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        fh = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    
    return logger

def get_last_access_days(stat_info, current_time):
    """
    Calculate the number of days since a file was last accessed.
    
    Uses the most recent of atime (access time) and mtime (modification time)
    as some filesystems don't update atime reliably.
    
    Args:
        stat_info (os.stat_result): File stat information from os.stat()
        current_time (datetime): Current time reference point
        
    Returns:
        int: Number of days since the file was last accessed
    """
    # Use the most recent of atime and mtime as the "last access"
    # Some filesystems don't update atime reliably
    last_access = max(
        datetime.fromtimestamp(stat_info.st_atime),
        datetime.fromtimestamp(stat_info.st_mtime)
    )
    return (current_time - last_access).days

def is_system_directory(path, system_dirs):
    """
    Check if a path is in or under a system directory that should be excluded.
    
    This function determines if a given path should be excluded from scanning
    by checking if it's within a system directory that typically causes issues
    or contains virtual files that aren't relevant for storage analysis.
    
    Args:
        path (Path or str): Path to check
        system_dirs (list): List of Path objects representing system directories to exclude
        
    Returns:
        bool: True if the path is in or under a system directory, False otherwise
    """
    path_str = str(path)
    
    # Check against system_dirs list
    if any(path_str.startswith(str(sys_dir)) for sys_dir in system_dirs):
        return True
    
    # Additional check for common system directories that should be skipped
    common_system_dirs = ['/proc', '/sys', '/dev', '/run', '/tmp', '/var/run']
    path_parts = Path(path_str).parts
    
    # Check if any part of the path matches a system directory
    return any(sys_dir.strip('/') in path_parts for sys_dir in common_system_dirs)

def scan_directory(directory, exclude_dirs, current_time, max_depth, current_depth,
                  follow_symlinks, system_dirs, visited_paths):
    """
    Scan a directory recursively and collect file statistics.
    
    This function traverses a directory structure, categorizing files by last access time
    and collecting statistics while handling errors and avoiding system directories.
    
    Args:
        directory (Path): Directory to scan
        exclude_dirs (list): List of directory names to exclude
        current_time (datetime): Current time reference point
        max_depth (int): Maximum recursion depth
        current_depth (int): Current recursion depth
        follow_symlinks (bool): Whether to follow symbolic links
        system_dirs (list): List of system directories to exclude
        visited_paths (set): Set of already visited paths to avoid loops
        
    Returns:
        FileStats: Object containing statistics about the scanned files
    """
    global processed_files, progress_bar
    
    stats = FileStats()
    subdirs = []

    # First check if this is a system directory we should skip entirely
    if is_system_directory(directory, system_dirs):
        if logger:
            logger.info(f"Skipping system directory: {directory}")
        return stats

    try:
        # Scan current directory
        try:
            dir_entries = list(os.scandir(directory))
        except (PermissionError, FileNotFoundError) as e:
            if logger:
                logger.error(f"Cannot access directory {directory}: {e}")
            stats.errors += 1
            return stats
            
        for entry in dir_entries:
            try:
                entry_path = Path(entry.path)
                
                # Skip excluded directories
                if entry.is_dir() and (entry.name in exclude_dirs or is_system_directory(entry_path, system_dirs)):
                    continue
                
                # Handle symlinks
                if entry.is_symlink() and not follow_symlinks:
                    continue
                
                # Detect symlink loops
                try:
                    real_path = entry_path.resolve()
                    if real_path in visited_paths:
                        continue
                    visited_paths.add(real_path)
                except (FileNotFoundError, PermissionError):
                    stats.errors += 1
                    continue
                
                # Process files
                try:
                    if entry.is_file(follow_symlinks=follow_symlinks):
                        try:
                            stat_info = entry.stat(follow_symlinks=follow_symlinks)
                            last_access_days = get_last_access_days(stat_info, current_time)
                            stats.add_file(stat_info.st_size, last_access_days)
                            
                            processed_files += 1
                            if progress_bar:
                                progress_bar.update(1)
                        except (FileNotFoundError, PermissionError, OSError) as e:
                            stats.errors += 1
                            if logger:
                                logger.error(f"Error accessing file {entry.path}: {e}")
                    
                    # Collect subdirectories for recursive scanning
                    elif entry.is_dir(follow_symlinks=follow_symlinks):
                        if not is_system_directory(entry_path, system_dirs):
                            subdirs.append(entry_path)
                except OSError as e:
                    stats.errors += 1
                    if logger:
                        logger.error(f"Error determining file type for {entry.path}: {e}")
            
            except Exception as e:
                stats.errors += 1
                if logger:
                    logger.error(f"Error processing entry {entry.path}: {e}")
    
    except Exception as e:
        if logger:
            logger.error(f"Error scanning directory {directory}: {e}")
    
    # Process subdirectories if not at max depth
    if current_depth < max_depth and subdirs:
        for subdir in subdirs:
            sub_stats = scan_directory(
                subdir, exclude_dirs, current_time, max_depth,
                current_depth + 1, follow_symlinks, system_dirs, visited_paths
            )
            stats.merge(sub_stats)
    
    return stats

def parallel_scan_directory(directory, exclude_dirs, current_time, max_depth, 
                           parallel, follow_symlinks, system_dirs):
    """
    Scan a directory using parallel processing for improved performance.
    
    This function coordinates parallel scanning of subdirectories using multiple processes,
    estimates the total number of files for progress tracking, and aggregates results.
    
    Args:
        directory (str or Path): Root directory to scan
        exclude_dirs (list): List of directory names to exclude
        current_time (datetime): Current time reference point
        max_depth (int): Maximum recursion depth
        parallel (int): Number of parallel processes to use
        follow_symlinks (bool): Whether to follow symbolic links
        system_dirs (list): List of system directories to exclude
        
    Returns:
        FileStats: Object containing aggregated statistics about the scanned files
    """
    global total_files, processed_files, start_time, progress_bar
    
    # Estimate total files for progress tracking
    print("Estimating total files (this may take a while for large filesystems)...")
    
    # Use a safer approach to estimate files that avoids permission errors
    total_files = 0
    try:
        # First check if we're scanning a system directory that should be excluded
        dir_path = Path(directory)
        if is_system_directory(dir_path, system_dirs):
            print(f"Warning: {directory} appears to be a system directory. This may cause errors.")
        
        # Use os.walk which handles permission errors better than Path.glob
        for root, _, files in os.walk(directory, topdown=True):
            # Skip excluded and system directories
            root_path = Path(root)
            if any(excluded in root_path.parts for excluded in exclude_dirs) or \
               is_system_directory(root_path, system_dirs):
                continue
                
            total_files += len(files)
            
            # Early exit if we've counted enough files for a reasonable estimate
            if total_files > 10000:
                # Multiply by a factor to estimate the total
                total_files = int(total_files * 1.5)
                print(f"Found over 10,000 files, estimating approximately {total_files} total files")
                break
    except Exception as e:
        print(f"Error estimating file count: {e}")
        # Use a default value if estimation fails
        total_files = 1000
        print("Using default file count estimate of 1,000 files")
    
    # Initialize progress tracking
    processed_files = 0
    start_time = time.time()
    progress_bar = tqdm(total=total_files, unit='files', dynamic_ncols=True)
    
    # First level scan to get subdirectories for parallel processing
    visited_paths = set()
    subdirs = []
    
    try:
        for entry in os.scandir(directory):
            if entry.is_dir() and entry.name not in exclude_dirs:
                if not is_system_directory(Path(entry.path), system_dirs):
                    subdirs.append(Path(entry.path))
    except Exception as e:
        if logger:
            logger.error(f"Error scanning top directory {directory}: {e}")
    
    # If no subdirectories or only one worker, do a single-process scan
    if not subdirs or parallel <= 1:
        stats = scan_directory(
            Path(directory), exclude_dirs, current_time, max_depth,
            0, follow_symlinks, system_dirs, visited_paths
        )
        progress_bar.close()
        return stats
    
    # Otherwise, process subdirectories in parallel
    stats = FileStats()
    with ProcessPoolExecutor(max_workers=min(parallel, len(subdirs))) as executor:
        futures = []
        
        # Submit jobs for each subdirectory
        for subdir in subdirs:
            future = executor.submit(
                scan_directory, subdir, exclude_dirs, current_time,
                max_depth, 1, follow_symlinks, system_dirs, set()
            )
            futures.append(future)
        
        # Process results as they complete
        for future in as_completed(futures):
            try:
                sub_stats = future.result()
                stats.merge(sub_stats)
            except Exception as e:
                if logger:
                    logger.error(f"Error in parallel processing: {e}")
    
    progress_bar.close()
    return stats

def calculate_costs(stats):
    """
    Calculate storage costs across different EFS tiers.
    
    This function analyzes file access patterns and calculates current costs
    (assuming all data is in Standard tier) versus optimized costs (using
    appropriate tiers based on access patterns).
    
    Tier assignment logic:
    - Standard tier: Files accessed within the last 7 days
    - Infrequent Access tier: Files accessed between 8-30 days ago
    - Archive tier: Files not accessed for more than 30 days
    
    Args:
        stats (FileStats): Statistics about the scanned files
        
    Returns:
        dict: Dictionary containing cost analysis results with the following keys:
            - total_gb: Total storage size in GB
            - current_cost: Current monthly cost (all in Standard tier)
            - optimized_cost: Optimized monthly cost using appropriate tiers
            - monthly_savings: Potential monthly savings
            - savings_percentage: Savings as a percentage of current cost
            - tier_distribution: Dictionary mapping tiers to GB allocated to each
    """
    # Convert bytes to GB
    gb_conversion = 1024 * 1024 * 1024
    
    # Current cost (all in standard tier)
    total_gb = stats.total_size / gb_conversion
    current_cost = total_gb * PRICING['standard']
    
    # Optimized cost based on access patterns
    optimized_cost = 0
    tier_distribution = {
        'standard': 0,
        'ia': 0,
        'archive': 0
    }
    
    # Files accessed in last 7 days: Standard tier
    standard_gb = stats.categories["0-7_days"] / gb_conversion
    optimized_cost += standard_gb * PRICING['standard']
    tier_distribution['standard'] = standard_gb
    
    # Files accessed between 8-30 days: IA tier
    ia_gb = (stats.categories["8-14_days"] + stats.categories["15-30_days"]) / gb_conversion
    optimized_cost += ia_gb * PRICING['ia']
    tier_distribution['ia'] = ia_gb
    
    # Files accessed over 30 days ago: Archive tier
    archive_gb = (stats.categories["31-60_days"] + stats.categories["61-90_days"] + 
                 stats.categories["91-365_days"] + stats.categories["1-2_years"] + 
                 stats.categories["2+_years"]) / gb_conversion
    optimized_cost += archive_gb * PRICING['archive']
    tier_distribution['archive'] = archive_gb
    
    # Calculate savings
    savings = current_cost - optimized_cost
    savings_percentage = (savings / current_cost * 100) if current_cost > 0 else 0
    
    return {
        'total_gb': total_gb,
        'current_cost': current_cost,
        'optimized_cost': optimized_cost,
        'monthly_savings': savings,
        'savings_percentage': savings_percentage,
        'tier_distribution': tier_distribution
    }

def generate_text_report(stats, cost_analysis):
    """
    Generate a plain text report with analysis results.
    
    Creates a formatted text report containing file statistics, cost analysis,
    and recommendations for storage optimization.
    
    Args:
        stats (FileStats): Statistics about the scanned files
        cost_analysis (dict): Cost analysis results from calculate_costs()
        
    Returns:
        str: Formatted text report
    """
    report = []
    report.append("=" * 80)
    report.append("EFS STORAGE OPTIMIZATION ANALYSIS REPORT")
    report.append("=" * 80)
    report.append("")
    
    # Summary statistics
    report.append("SUMMARY STATISTICS")
    report.append("-" * 80)
    report.append(f"Total files scanned: {stats.total_files:,}")
    report.append(f"Total storage size: {humanize.naturalsize(stats.total_size)}")
    report.append(f"Scan errors: {stats.errors}")
    report.append("")
    
    # File access statistics
    report.append("FILE ACCESS STATISTICS")
    report.append("-" * 80)
    for category, size in stats.categories.items():
        report.append(f"{category}: {humanize.naturalsize(size)} ({size/stats.total_size*100:.1f}%)")
    report.append("")
    
    # Cost analysis
    report.append("COST ANALYSIS")
    report.append("-" * 80)
    report.append(f"Current monthly cost (all Standard tier): ${cost_analysis['current_cost']:.2f}")
    report.append(f"Optimized monthly cost: ${cost_analysis['optimized_cost']:.2f}")
    report.append(f"Potential monthly savings: ${cost_analysis['monthly_savings']:.2f} ({cost_analysis['savings_percentage']:.1f}%)")
    report.append("")
    
    # Tier distribution recommendations
    report.append("RECOMMENDED TIER DISTRIBUTION")
    report.append("-" * 80)
    for tier, gb in cost_analysis['tier_distribution'].items():
        report.append(f"{tier.capitalize()} tier: {gb:.2f} GB (${gb * PRICING[tier]:.2f}/month)")
    report.append("")
    
    # Recommendations
    report.append("RECOMMENDATIONS")
    report.append("-" * 80)
    if cost_analysis['savings_percentage'] > 20:
        report.append("✅ SIGNIFICANT SAVINGS OPPORTUNITY: Consider implementing lifecycle policies")
        report.append("   to automatically transition data between storage tiers based on access patterns.")
    elif cost_analysis['savings_percentage'] > 5:
        report.append("✅ MODERATE SAVINGS OPPORTUNITY: Review your data access patterns and consider")
        report.append("   implementing lifecycle policies for less frequently accessed data.")
    else:
        report.append("✅ MINIMAL SAVINGS OPPORTUNITY: Your current storage usage appears optimized.")
    
    if stats.categories["2+_years"] / stats.total_size > 0.3:
        report.append("✅ CONSIDER ARCHIVING: More than 30% of your data hasn't been accessed in over 2 years.")
        report.append("   Consider moving this data to Archive tier or S3 Glacier for long-term storage.")
    
    return "\n".join(report)

def generate_html_report(stats, cost_analysis, output_path):
    """
    Generate an HTML report with visualizations.
    
    Creates an HTML report with charts and tables showing file statistics,
    cost analysis, and recommendations for storage optimization.
    
    Args:
        stats (FileStats): Statistics about the scanned files
        cost_analysis (dict): Cost analysis results from calculate_costs()
        output_path (str): Path where the HTML report will be saved
        
    Returns:
        str: Path to the generated HTML report
    """
    # Create pie chart for access time distribution
    plt.figure(figsize=(10, 6))
    labels = []
    sizes = []
    for category, size in stats.categories.items():
        if size > 0:
            labels.append(category)
            sizes.append(size)
    
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    plt.axis('equal')
    plt.title('Storage Distribution by Last Access Time')
    chart_path = os.path.join(os.path.dirname(output_path), 'access_time_chart.png')
    plt.savefig(chart_path)
    
    # Create bar chart for tier cost comparison
    plt.figure(figsize=(10, 6))
    tiers = ['Current (Standard)', 'Optimized']
    costs = [cost_analysis['current_cost'], cost_analysis['optimized_cost']]
    plt.bar(tiers, costs)
    plt.ylabel('Monthly Cost ($)')
    plt.title('Cost Comparison: Current vs. Optimized')
    cost_chart_path = os.path.join(os.path.dirname(output_path), 'cost_comparison_chart.png')
    plt.savefig(cost_chart_path)
    
    # Create HTML template
    template_str = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>EFS Storage Optimization Analysis</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            h1, h2 { color: #0066cc; }
            .summary { background-color: #f0f0f0; padding: 15px; border-radius: 5px; }
            .savings { font-size: 24px; color: #009900; }
            .chart { margin: 20px 0; }
            table { border-collapse: collapse; width: 100%; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #0066cc; color: white; }
            tr:nth-child(even) { background-color: #f2f2f2; }
        </style>
    </head>
    <body>
        <h1>EFS Storage Optimization Analysis Report</h1>
        
        <div class="summary">
            <h2>Summary</h2>
            <p>Total files scanned: {{ stats.total_files }}</p>
            <p>Total storage size: {{ total_size_human }}</p>
            <p>Scan errors: {{ stats.errors }}</p>
            <p class="savings">Potential monthly savings: ${{ "%.2f"|format(cost_analysis.monthly_savings) }} ({{ "%.1f"|format(cost_analysis.savings_percentage) }}%)</p>
        </div>
        
        <h2>Storage Distribution by Last Access Time</h2>
        <div class="chart">
            <img src="access_time_chart.png" alt="Access Time Distribution Chart" width="600">
        </div>
        
        <h2>File Access Statistics</h2>
        <table>
            <tr>
                <th>Access Time Category</th>
                <th>Size</th>
                <th>Percentage</th>
            </tr>
            {% for category, size in stats.categories.items() %}
            <tr>
                <td>{{ category }}</td>
                <td>{{ humanize.naturalsize(size) }}</td>
                <td>{{ "%.1f"|format(size/stats.total_size*100) }}%</td>
            </tr>
            {% endfor %}
        </table>
        
        <h2>Cost Comparison</h2>
        <div class="chart">
            <img src="cost_comparison_chart.png" alt="Cost Comparison Chart" width="600">
        </div>
        
        <h2>Cost Analysis</h2>
        <table>
            <tr>
                <th>Metric</th>
                <th>Value</th>
            </tr>
            <tr>
                <td>Current monthly cost (all Standard tier)</td>
                <td>${{ "%.2f"|format(cost_analysis.current_cost) }}</td>
            </tr>
            <tr>
                <td>Optimized monthly cost</td>
                <td>${{ "%.2f"|format(cost_analysis.optimized_cost) }}</td>
            </tr>
            <tr>
                <td>Potential monthly savings</td>
                <td>${{ "%.2f"|format(cost_analysis.monthly_savings) }} ({{ "%.1f"|format(cost_analysis.savings_percentage) }}%)</td>
            </tr>
        </table>
        
        <h2>Recommended Tier Distribution</h2>
        <table>
            <tr>
                <th>Storage Tier</th>
                <th>Size (GB)</th>
                <th>Monthly Cost</th>
            </tr>
            {% for tier, gb in cost_analysis.tier_distribution.items() %}
            <tr>
                <td>{{ tier|capitalize }}</td>
                <td>{{ "%.2f"|format(gb) }}</td>
                <td>${{ "%.2f"|format(gb * pricing[tier]) }}</td>
            </tr>
            {% endfor %}
        </table>
        
        <h2>Recommendations</h2>
        <ul>
            {% if cost_analysis.savings_percentage > 20 %}
            <li>SIGNIFICANT SAVINGS OPPORTUNITY: Consider implementing lifecycle policies
                to automatically transition data between storage tiers based on access patterns.</li>
            {% elif cost_analysis.savings_percentage > 5 %}
            <li>MODERATE SAVINGS OPPORTUNITY: Review your data access patterns and consider
                implementing lifecycle policies for less frequently accessed data.</li>
            {% else %}
            <li>MINIMAL SAVINGS OPPORTUNITY: Your current storage usage appears optimized.</li>
            {% endif %}
            
            {% if stats.categories["2+_years"] / stats.total_size > 0.3 %}
            <li>CONSIDER ARCHIVING: More than 30% of your data hasn't been accessed in over 2 years.
                Consider moving this data to Archive tier or S3 Glacier for long-term storage.</li>
            {% endif %}
        </ul>
    </body>
    </html>
    """
    
    # Render template
    template = jinja2.Template(template_str)
    html_content = template.render(
        stats=stats,
        total_size_human=humanize.naturalsize(stats.total_size),
        cost_analysis=cost_analysis,
        humanize=humanize,
        pricing=PRICING
    )
    
    # Write HTML file
    with open(output_path, 'w') as f:
        f.write(html_content)
    
    return output_path

def print_banner():
    """
    Print a banner with information about the EFS Analyzer tool.
    """
    banner = """
    ┌─────────────────────────────────────────────────────────┐
    │                    EFS ANALYZER TOOL                    │
    ├─────────────────────────────────────────────────────────┤
    │                                                         │
    │  This tool analyzes Amazon EFS mount points to identify │
    │  cost optimization opportunities by:                    │
    │                                                         │
    │  • Scanning files and categorizing by last access time  │
    │  • Calculating potential savings across storage tiers   │
    │  • Generating detailed reports with recommendations     │
    │                                                         │
    │  NOTE: This tool is READ-ONLY and will not modify any   │
    │        files or change your EFS configuration.          │
    │                                                         │
    └─────────────────────────────────────────────────────────┘
    """
    print(banner)

def print_examples():
    """
    Print usage examples for the EFS Analyzer tool.
    """
    examples = """
EXAMPLES:

Basic usage:
    python efs_analyzer.py /mnt/efs

Analyze root directory (requires sudo/admin):
    sudo python efs_analyzer.py / --skip-estimate

Specify parallel processes:
    python efs_analyzer.py /mnt/efs --parallel 8

Exclude specific directories:
    python efs_analyzer.py /mnt/efs --exclude cache temp logs

Limit scan depth:
    python efs_analyzer.py /mnt/efs --max-depth 10

Follow symbolic links:
    python efs_analyzer.py /mnt/efs --follow-symlinks

Custom output directory:
    python efs_analyzer.py /mnt/efs --output-dir /path/to/reports

For more information, use the --help option.
"""
    print(examples)

def confirm_proceed():
    """
    Ask the user for confirmation before proceeding with the scan.
    
    Returns:
        bool: True if the user confirms, False otherwise
    """
    while True:
        response = input("\nThis tool will perform a READ-ONLY scan of your filesystem.\n"
                         "No changes will be made to your files or EFS configuration.\n"
                         "Do you want to proceed? [y/n]: ").lower().strip()
        
        if response in ('y', 'yes'):
            return True
        elif response in ('n', 'no'):
            return False
        else:
            print("Please enter 'y' for yes or 'n' for no.")

def main():
    """
    Main entry point for the EFS Analyzer script.
    
    Parses command-line arguments, sets up logging, and coordinates the
    scanning, analysis, and report generation process.
    
    Returns:
        int: Exit code (0 for success, 1 for error)
    """
    parser = argparse.ArgumentParser(
        description="EFS Analyzer - Analyzes EFS mount points for cost optimization opportunities",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog="This tool performs READ-ONLY operations and does not modify any files."
    )
    
    parser.add_argument("mount_point", help="EFS mount point to analyze")
    parser.add_argument("--parallel", type=int, default=multiprocessing.cpu_count(),
                      help="Number of parallel processes to use")
    parser.add_argument("--exclude", nargs='+', 
                      default=['proc', 'sys', 'dev', 'run', 'tmp', 'mnt', 'media'],
                      help="Directories to exclude from analysis")
    parser.add_argument("--max-depth", type=int, default=100,
                      help="Maximum directory depth to scan")
    parser.add_argument("--follow-symlinks", action="store_true",
                      help="Follow symbolic links (use with caution)")
    parser.add_argument("--output-dir", default="./reports",
                      help="Directory to store reports")
    parser.add_argument("--log-file", default="efs_analyzer.log",
                      help="Log file for errors and warnings")
    parser.add_argument("--skip-estimate", action="store_true",
                      help="Skip initial file count estimation (faster start)")
    parser.add_argument("--examples", action="store_true",
                      help="Show usage examples and exit")
    parser.add_argument("--yes", "-y", action="store_true",
                      help="Skip confirmation prompt and proceed with scan")
    
    args = parser.parse_args()
    
    # Print banner with tool information
    print_banner()
    
    # Show examples if requested
    if args.examples:
        print_examples()
        return 0
        
    # Ask for confirmation unless --yes flag is used
    if not args.yes and not confirm_proceed():
        print("Scan cancelled by user.")
        return 0
    
    # Setup logging
    global logger
    logger = setup_logging(args.log_file)
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Define system directories to exclude
    system_dirs = []
    if platform.system() != "Windows":
        system_dirs = [
            Path("/proc"), Path("/sys"), Path("/dev"), Path("/run"),
            Path("/tmp"), Path("/mnt"), Path("/media")
        ]
        
    # Add excluded directories to system_dirs
    for excluded in args.exclude:
        if not any(str(excluded) in str(sys_dir) for sys_dir in system_dirs):
            system_dirs.append(Path(f"/{excluded}"))
    
    print(f"Starting EFS analysis of {args.mount_point}")
    print(f"Using {args.parallel} parallel processes")
    
    # Check if mount point exists
    if not os.path.exists(args.mount_point):
        print(f"Error: Mount point {args.mount_point} does not exist")
        return 1
        
    # Check if mount point is accessible
    if not os.access(args.mount_point, os.R_OK):
        print(f"Error: Mount point {args.mount_point} is not readable")
        print("Try running the script with sudo or as root")
        return 1
    
    # Start the scan
    current_time = datetime.now()
    
    # Skip estimation if requested
    global total_files, progress_bar
    if args.skip_estimate:
        print("Skipping file count estimation")
        total_files = 1000  # Default value
        processed_files = 0
        start_time = time.time()
        progress_bar = tqdm(total=None, unit='files', dynamic_ncols=True)  # Indeterminate progress bar
        
        stats = scan_directory(
            Path(args.mount_point), args.exclude, current_time,
            args.max_depth, 0, args.follow_symlinks, system_dirs, set()
        )
    else:
        # Use parallel scan with estimation
        stats = parallel_scan_directory(
            args.mount_point, args.exclude, current_time,
            args.max_depth, args.parallel, args.follow_symlinks, system_dirs
        )
    
    # Calculate costs and savings
    cost_analysis = calculate_costs(stats)
    
    # Generate timestamp for report filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Generate text report
    text_report = generate_text_report(stats, cost_analysis)
    text_report_path = os.path.join(args.output_dir, f"efs_report_{timestamp}.txt")
    with open(text_report_path, 'w') as f:
        f.write(text_report)
    
    # Generate HTML report
    html_report_path = os.path.join(args.output_dir, f"efs_report_{timestamp}.html")
    generate_html_report(stats, cost_analysis, html_report_path)
    
    # Print summary to console
    print("\nAnalysis complete!")
    print(f"Total files scanned: {stats.total_files:,}")
    print(f"Total storage size: {humanize.naturalsize(stats.total_size)}")
    print(f"Potential monthly savings: ${cost_analysis['monthly_savings']:.2f} ({cost_analysis['savings_percentage']:.1f}%)")
    print(f"\nReports saved to:")
    print(f"  - Text report: {text_report_path}")
    print(f"  - HTML report: {html_report_path}")
    
    print("\nNOTE: No changes have been made to your filesystem or EFS configuration.")
    print("To implement the recommended optimizations, use the AWS Management Console")
    print("or AWS CLI to configure lifecycle policies for your EFS filesystem.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())