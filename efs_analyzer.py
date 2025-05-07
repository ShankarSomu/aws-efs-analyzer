#!/usr/bin/env python3
"""
EFS Analyzer - Analyzes EFS mount points for cost optimization opportunities

This script scans an EFS mount point, categorizes files based on last access time,
and calculates potential cost savings by moving data to different storage tiers.
"""

import os
import time
import datetime
import argparse
import json
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict, Counter
from pathlib import Path
import humanize
import threading
import tqdm
import sys
import queue
import signal

# Configure logging
def setup_logging(error_file=None):
    """Set up logging with separate handlers for console and error file."""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    
    # Clear any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create formatters
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Console handler - only show INFO and above, but not WARNING or ERROR
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(lambda record: record.levelno < logging.WARNING)
    
    # Error file handler - only show WARNING and above
    if error_file:
        file_handler = logging.FileHandler(error_file)
        file_handler.setLevel(logging.WARNING)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    root_logger.addHandler(console_handler)
    return logging.getLogger('efs-analyzer')

# Initialize logger (will be properly configured in main())
logger = logging.getLogger('efs-analyzer')

# EFS pricing (per GB-month in USD)
EFS_PRICING = {
    'standard': 0.30,  # Standard storage
    'ia': 0.025,       # Infrequent Access
    'archive': 0.016   # Archive
}

# Access time categories in days
ACCESS_CATEGORIES = {
    'last_7_days': 7,
    'last_14_days': 14,
    'last_30_days': 30,
    'last_60_days': 60,
    'last_90_days': 90,
    'last_1_year': 365,
    'last_2_years': 730,
    'older': float('inf')
}

def categorize_file(days_since_access):
    """
    Categorize a file based on days since last access.
    
    Args:
        days_since_access (float): Days since last access
        
    Returns:
        str: Category name
    """
    for category, threshold in sorted(ACCESS_CATEGORIES.items(), key=lambda x: x[1]):
        if days_since_access <= threshold:
            return category
    return 'older'

def process_file(file_path, current_time):
    """
    Process a file and return its statistics.
    
    Args:
        file_path (Path): Path to the file
        current_time (float): Current timestamp
        
    Returns:
        tuple: (category, file_size) or None if error
    """
    try:
        stat_info = file_path.stat()
        file_size = stat_info.st_size
        last_access_time = stat_info.st_atime
        
        # Calculate days since last access
        days_since_access = (current_time - last_access_time) / (24 * 3600)
        
        # Categorize the file
        category = categorize_file(days_since_access)
        
        return (category, file_size)
        
    except Exception as e:
        logger.debug(f"Error processing file {file_path}: {e}")
        return None

def scan_directory(directory, exclude_dirs, current_time, max_depth=None, current_depth=0, parallel=1):
    """
    Scan a directory and collect file statistics.
    
    Args:
        directory (Path): Directory to scan
        exclude_dirs (list): List of directories to exclude
        current_time (float): Current timestamp
        max_depth (int): Maximum depth to scan
        current_depth (int): Current depth
        parallel (int): Number of parallel processes to use for file processing
        
    Returns:
        list: List of (category, file_size) tuples
    """
    results = []
    
    # Check if we've reached the maximum depth
    if max_depth is not None and current_depth > max_depth:
        return results
    
    try:
        # First collect all files and directories
        files = []
        subdirs = []
        
        for item in directory.iterdir():
            # Skip excluded directories
            if item.is_dir() and any(exclude in str(item) for exclude in exclude_dirs):
                logger.info(f"Skipping excluded directory: {item}")
                continue
            
            if item.is_file():
                files.append(item)
            elif item.is_dir():
                subdirs.append(item)
        
        # Process files in parallel if there are enough files
        if len(files) > 10 and parallel > 1:
            with ProcessPoolExecutor(max_workers=min(parallel, len(files))) as executor:
                file_futures = {executor.submit(process_file, file_path, current_time): file_path 
                               for file_path in files}
                
                for future in as_completed(file_futures):
                    try:
                        result = future.result()
                        if result:
                            results.append(result)
                    except Exception as e:
                        logger.debug(f"Error processing file: {e}")
        else:
            # Process files sequentially for small directories
            for file_path in files:
                result = process_file(file_path, current_time)
                if result:
                    results.append(result)
        
        # Process subdirectories recursively
        for subdir in subdirs:
            results.extend(scan_directory(
                subdir, exclude_dirs, current_time, 
                max_depth, current_depth + 1, parallel
            ))
            
    except PermissionError:
        logger.warning(f"Permission denied: {directory}")
    except Exception as e:
        logger.warning(f"Error scanning {directory}: {e}")
    
    return results

def worker_scan_directory(args):
    """
    Worker function for parallel directory scanning.
    
    Args:
        args (tuple): (directory, exclude_dirs, current_time, max_depth, worker_id, parallel)
        
    Returns:
        dict: Dictionary with category counts and sizes
    """
    directory, exclude_dirs, current_time, max_depth, worker_id, parallel = args
    
    # Use internal parallelism for large directories
    results = scan_directory(directory, exclude_dirs, current_time, max_depth, 0, parallel)
    
    # Aggregate results
    stats = {category: {'count': 0, 'size': 0} for category in ACCESS_CATEGORIES}
    file_count = 0
    for category, file_size in results:
        stats[category]['count'] += 1
        stats[category]['size'] += file_size
        file_count += 1
    
    return stats, worker_id, str(directory), file_count

class EFSAnalyzer:
    """Analyzes EFS mount points for cost optimization opportunities."""
    
    def __init__(self, mount_path, output_dir=None, exclude_dirs=None, parallel=None, max_depth=None):
        """
        Initialize the EFS Analyzer.
        
        Args:
            mount_path (str): Path to the EFS mount point
            output_dir (str): Directory to save reports
            exclude_dirs (list): List of directories to exclude from analysis
            parallel (int): Number of parallel processes to use
            max_depth (int): Maximum directory depth to scan
        """
        self.mount_path = Path(mount_path)
        self.output_dir = Path(output_dir) if output_dir else Path.cwd()
        self.exclude_dirs = exclude_dirs or []
        self.parallel = parallel or multiprocessing.cpu_count()
        self.max_depth = max_depth
        self.stats = {category: {'count': 0, 'size': 0} for category in ACCESS_CATEGORIES}
        self.current_time = time.time()
        self._lock = threading.Lock()  # For thread-safe updates to stats
        self.total_files_scanned = 0  # Counter for total files scanned
        
    def analyze(self):
        """Analyze the EFS mount point."""
        logger.info(f"Starting analysis of {self.mount_path} with {self.parallel} parallel processes")
        logger.warning(f"Permission denied and error messages will be written to the error log file")
        start_time = time.time()
        
        try:
            # Get top-level directories for parallel processing
            top_dirs = []
            root_files = []
            try:
                # First, identify files and directories in the root
                print(f"Scanning root directory: {self.mount_path}")
                for item in self.mount_path.iterdir():
                    if item.is_file():
                        root_files.append(item)
                    elif item.is_dir() and not any(exclude in str(item) for exclude in self.exclude_dirs):
                        top_dirs.append(item)
                
                # Process root files in parallel
                if root_files:
                    print(f"Processing {len(root_files)} files in root directory")
                    with ProcessPoolExecutor(max_workers=self.parallel) as executor:
                        file_futures = {executor.submit(process_file, file_path, self.current_time): file_path 
                                       for file_path in root_files}
                        
                        root_progress = tqdm.tqdm(
                            total=len(root_files),
                            desc=f"Processing root files (0/{len(root_files)})",
                            unit="file",
                            bar_format="{desc}: {percentage:3.1f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
                        )
                        
                        for future in as_completed(file_futures):
                            try:
                                result = future.result()
                                if result:
                                    category, file_size = result
                                    with self._lock:
                                        self.stats[category]['count'] += 1
                                        self.stats[category]['size'] += file_size
                                        self.total_files_scanned += 1
                                        root_progress.set_description(f"Processing root files ({self.total_files_scanned}/{len(root_files)})")
                            except Exception as e:
                                logger.debug(f"Error processing root file: {e}")
                            finally:
                                root_progress.update(1)
                        
                        root_progress.close()
            except Exception as e:
                logger.error(f"Error processing root directory: {e}")
            
            # If no subdirectories found, process the mount point directly
            if not top_dirs:
                logger.info("No subdirectories found, processing mount point directly")
                top_dirs = [self.mount_path]
            
            # Prepare arguments for parallel processing
            args_list = [
                (directory, self.exclude_dirs, self.current_time, self.max_depth, i, max(1, self.parallel // 4))
                for i, directory in enumerate(top_dirs)
            ]
            
            total_dirs = len(top_dirs)
            print(f"Found {total_dirs} directories to analyze")
            
            # Create a progress bar in the main process
            progress_bar = tqdm.tqdm(
                total=total_dirs,
                desc=f"Analyzing directories (0/{total_dirs}) - Files: {self.total_files_scanned:,}",
                unit="dir",
                bar_format="{desc}: {percentage:3.1f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
            )
            
            # Use process pool for parallel scanning
            with ProcessPoolExecutor(max_workers=self.parallel) as executor:
                # Submit all tasks
                future_to_dir = {
                    executor.submit(worker_scan_directory, args): args[0]
                    for args in args_list
                }
                
                # Process results as they complete
                for future in as_completed(future_to_dir):
                    try:
                        dir_stats, worker_id, dir_name, files_processed = future.result()
                        
                        # Merge directory stats into overall stats
                        with self._lock:
                            for category, data in dir_stats.items():
                                self.stats[category]['count'] += data['count']
                                self.stats[category]['size'] += data['size']
                            self.total_files_scanned += files_processed
                        
                        # Update progress bar
                        progress_bar.update(1)
                        progress_bar.set_description(f"Analyzing directories ({progress_bar.n}/{total_dirs}) - Files: {self.total_files_scanned:,}")
                    except Exception as e:
                        logger.error(f"Error processing directory: {e}")
                        progress_bar.update(1)  # Still update progress even if there was an error
            
            # Close progress bar
            progress_bar.close()
            
            elapsed_time = time.time() - start_time
            print(f"\nAnalysis completed in {elapsed_time:.2f} seconds")
            print(f"Total files scanned: {self.total_files_scanned:,}")
            print(f"Total storage: {humanize.naturalsize(sum(cat['size'] for cat in self.stats.values()))}")
            return self.stats
        except Exception as e:
            logger.error(f"Error during analysis: {e}")
            raise
        finally:
            # Make sure we close the progress bar in case of exceptions
            if 'progress_bar' in locals():
                progress_bar.close()
    
    def calculate_costs(self):
        """
        Calculate storage costs for different EFS tiers.
        
        Returns:
            dict: Cost calculations
        """
        costs = {}
        total_size_gb = sum(cat['size'] for cat in self.stats.values()) / (1024 ** 3)
        
        # Calculate current cost (assuming all in standard)
        costs['current'] = total_size_gb * EFS_PRICING['standard']
        
        # Calculate optimized cost
        optimized_cost = 0
        tier_sizes = {'standard': 0, 'ia': 0, 'archive': 0}
        
        # Files accessed within 30 days stay in standard
        for category in ['last_7_days', 'last_14_days', 'last_30_days']:
            size_gb = self.stats[category]['size'] / (1024 ** 3)
            tier_sizes['standard'] += size_gb
            optimized_cost += size_gb * EFS_PRICING['standard']
        
        # Files accessed between 30-90 days go to IA
        for category in ['last_60_days', 'last_90_days']:
            size_gb = self.stats[category]['size'] / (1024 ** 3)
            tier_sizes['ia'] += size_gb
            optimized_cost += size_gb * EFS_PRICING['ia']
        
        # Files older than 90 days go to Archive
        for category in ['last_1_year', 'last_2_years', 'older']:
            size_gb = self.stats[category]['size'] / (1024 ** 3)
            tier_sizes['archive'] += size_gb
            optimized_cost += size_gb * EFS_PRICING['archive']
        
        costs['optimized'] = optimized_cost
        costs['savings'] = costs['current'] - costs['optimized']
        costs['savings_percent'] = (costs['savings'] / costs['current'] * 100) if costs['current'] > 0 else 0
        costs['tier_sizes'] = tier_sizes
        
        return costs
    
    def generate_text_report(self):
        """
        Generate a plain text report.
        
        Returns:
            str: Report content
        """
        costs = self.calculate_costs()
        
        report = []
        report.append("=" * 80)
        report.append("EFS ANALYZER REPORT")
        report.append("=" * 80)
        report.append(f"Mount Point: {self.mount_path}")
        report.append(f"Analysis Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Parallel Processes: {self.parallel}")
        if self.max_depth is not None:
            report.append(f"Maximum Scan Depth: {self.max_depth}")
        report.append(f"Total Files Scanned: {self.total_files_scanned:,}")
        report.append("")
        
        report.append("FILE ACCESS STATISTICS")
        report.append("-" * 80)
        report.append(f"{'Category':<20} {'Count':<10} {'Size':<15} {'Percentage':<10}")
        report.append("-" * 80)
        
        total_size = sum(cat['size'] for cat in self.stats.values())
        for category, data in self.stats.items():
            size_human = humanize.naturalsize(data['size'])
            percentage = (data['size'] / total_size * 100) if total_size > 0 else 0
            report.append(f"{category:<20} {data['count']:<10} {size_human:<15} {percentage:.2f}%")
        
        report.append("")
        report.append("COST ANALYSIS")
        report.append("-" * 80)
        report.append(f"Total Storage: {humanize.naturalsize(total_size)}")
        report.append(f"Current Monthly Cost (Standard tier): ${costs['current']:.2f}")
        report.append(f"Optimized Monthly Cost: ${costs['optimized']:.2f}")
        report.append(f"Monthly Savings: ${costs['savings']:.2f} ({costs['savings_percent']:.2f}%)")
        report.append("")
        
        report.append("RECOMMENDED TIER DISTRIBUTION")
        report.append("-" * 80)
        report.append(f"Standard tier: {humanize.naturalsize(costs['tier_sizes']['standard'] * (1024 ** 3))}")
        report.append(f"Infrequent Access tier: {humanize.naturalsize(costs['tier_sizes']['ia'] * (1024 ** 3))}")
        report.append(f"Archive tier: {humanize.naturalsize(costs['tier_sizes']['archive'] * (1024 ** 3))}")
        report.append("")
        
        report.append("RECOMMENDATIONS")
        report.append("-" * 80)
        report.append("1. Consider moving files not accessed in the last 30 days to Infrequent Access tier")
        report.append("2. Consider moving files not accessed in the last 90 days to Archive tier")
        report.append("3. Implement lifecycle policies to automatically transition files between tiers")
        report.append("")
        
        return "\n".join(report)
    
    def generate_html_report(self):
        """
        Generate an HTML report.
        
        Returns:
            str: HTML report content
        """
        costs = self.calculate_costs()
        total_size = sum(cat['size'] for cat in self.stats.values())
        
        html = []
        html.append("<!DOCTYPE html>")
        html.append("<html lang='en'>")
        html.append("<head>")
        html.append("    <meta charset='UTF-8'>")
        html.append("    <meta name='viewport' content='width=device-width, initial-scale=1.0'>")
        html.append("    <title>EFS Analyzer Report</title>")
        html.append("    <style>")
        html.append("        body { font-family: Arial, sans-serif; margin: 20px; }")
        html.append("        h1, h2 { color: #0073bb; }")
        html.append("        table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }")
        html.append("        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }")
        html.append("        th { background-color: #f2f2f2; }")
        html.append("        tr:nth-child(even) { background-color: #f9f9f9; }")
        html.append("        .savings { color: green; font-weight: bold; }")
        html.append("        .summary { background-color: #f0f7fb; padding: 15px; border-left: 5px solid #0073bb; }")
        html.append("    </style>")
        html.append("</head>")
        html.append("<body>")
        
        html.append("    <h1>EFS Analyzer Report</h1>")
        html.append(f"    <p><strong>Mount Point:</strong> {self.mount_path}</p>")
        html.append(f"    <p><strong>Analysis Date:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>")
        html.append(f"    <p><strong>Parallel Processes:</strong> {self.parallel}</p>")
        if self.max_depth is not None:
            html.append(f"    <p><strong>Maximum Scan Depth:</strong> {self.max_depth}</p>")
        html.append(f"    <p><strong>Total Files Scanned:</strong> {self.total_files_scanned:,}</p>")
        
        html.append("    <div class='summary'>")
        html.append("        <h2>Summary</h2>")
        html.append(f"        <p>Total Storage: <strong>{humanize.naturalsize(total_size)}</strong></p>")
        html.append(f"        <p>Current Monthly Cost: <strong>${costs['current']:.2f}</strong></p>")
        html.append(f"        <p>Optimized Monthly Cost: <strong>${costs['optimized']:.2f}</strong></p>")
        html.append(f"        <p>Potential Monthly Savings: <span class='savings'>${costs['savings']:.2f} ({costs['savings_percent']:.2f}%)</span></p>")
        html.append("    </div>")
        
        html.append("    <h2>File Access Statistics</h2>")
        html.append("    <table>")
        html.append("        <tr>")
        html.append("            <th>Category</th>")
        html.append("            <th>File Count</th>")
        html.append("            <th>Size</th>")
        html.append("            <th>Percentage</th>")
        html.append("        </tr>")
        
        for category, data in self.stats.items():
            size_human = humanize.naturalsize(data['size'])
            percentage = (data['size'] / total_size * 100) if total_size > 0 else 0
            html.append("        <tr>")
            html.append(f"            <td>{category.replace('_', ' ').title()}</td>")
            html.append(f"            <td>{data['count']:,}</td>")
            html.append(f"            <td>{size_human}</td>")
            html.append(f"            <td>{percentage:.2f}%</td>")
            html.append("        </tr>")
        
        html.append("    </table>")
        
        html.append("    <h2>Recommended Tier Distribution</h2>")
        html.append("    <table>")
        html.append("        <tr>")
        html.append("            <th>Storage Tier</th>")
        html.append("            <th>Size</th>")
        html.append("            <th>Monthly Cost</th>")
        html.append("        </tr>")
        
        html.append("        <tr>")
        html.append("            <td>Standard</td>")
        html.append(f"            <td>{humanize.naturalsize(costs['tier_sizes']['standard'] * (1024 ** 3))}</td>")
        html.append(f"            <td>${costs['tier_sizes']['standard'] * EFS_PRICING['standard']:.2f}</td>")
        html.append("        </tr>")
        
        html.append("        <tr>")
        html.append("            <td>Infrequent Access</td>")
        html.append(f"            <td>{humanize.naturalsize(costs['tier_sizes']['ia'] * (1024 ** 3))}</td>")
        html.append(f"            <td>${costs['tier_sizes']['ia'] * EFS_PRICING['ia']:.2f}</td>")
        html.append("        </tr>")
        
        html.append("        <tr>")
        html.append("            <td>Archive</td>")
        html.append(f"            <td>{humanize.naturalsize(costs['tier_sizes']['archive'] * (1024 ** 3))}</td>")
        html.append(f"            <td>${costs['tier_sizes']['archive'] * EFS_PRICING['archive']:.2f}</td>")
        html.append("        </tr>")
        
        html.append("    </table>")
        
        html.append("    <h2>Recommendations</h2>")
        html.append("    <ol>")
        html.append("        <li>Consider moving files not accessed in the last 30 days to Infrequent Access tier</li>")
        html.append("        <li>Consider moving files not accessed in the last 90 days to Archive tier</li>")
        html.append("        <li>Implement lifecycle policies to automatically transition files between tiers</li>")
        html.append("    </ol>")
        
        html.append("    <h2>Cost Comparison Chart</h2>")
        html.append("    <p>The chart below shows the cost comparison between current and optimized storage configurations:</p>")
        
        # Add a simple bar chart using HTML/CSS
        html.append("    <div style='width: 100%; height: 300px; position: relative;'>")
        html.append("        <div style='position: absolute; bottom: 0; left: 0; width: 100px; height: 100%;'>")
        html.append("            <div style='position: absolute; bottom: 0; width: 40px; height: 100%; background-color: #0073bb;'></div>")
        html.append("            <div style='position: absolute; bottom: 0; left: 50px; width: 40px; height: " + 
                    f"{(costs['optimized'] / costs['current'] * 100) if costs['current'] > 0 else 0}%" + 
                    "; background-color: #00bb73;'></div>")
        html.append("        </div>")
        html.append("        <div style='position: absolute; bottom: -30px; left: 0;'>Current</div>")
        html.append("        <div style='position: absolute; bottom: -30px; left: 50px;'>Optimized</div>")
        html.append("    </div>")
        
        html.append("</body>")
        html.append("</html>")
        
        return "\n".join(html)
    
    def save_reports(self):
        """Save reports to files."""
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save text report
        text_report = self.generate_text_report()
        text_path = self.output_dir / f"efs_analysis_{timestamp}.txt"
        with open(text_path, 'w') as f:
            f.write(text_report)
        logger.info(f"Text report saved to {text_path}")
        
        # Save HTML report
        html_report = self.generate_html_report()
        html_path = self.output_dir / f"efs_analysis_{timestamp}.html"
        with open(html_path, 'w') as f:
            f.write(html_report)
        logger.info(f"HTML report saved to {html_path}")
        
        return text_path, html_path

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Analyze EFS mount points for cost optimization')
    parser.add_argument('mount_path', help='Path to the EFS mount point')
    parser.add_argument('--output-dir', '-o', help='Directory to save reports')
    parser.add_argument('--exclude', '-e', nargs='+', help='Directories to exclude from analysis')
    parser.add_argument('--parallel', '-p', type=int, help='Number of parallel processes (default: number of CPUs)')
    parser.add_argument('--max-depth', '-d', type=int, help='Maximum directory depth to scan')
    parser.add_argument('--error-log', help='File to write warnings and errors (default: efs_analyzer_errors.log)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set up error log file
    error_log = args.error_log or "efs_analyzer_errors.log"
    global logger
    logger = setup_logging(error_log)
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    try:
        # Handle keyboard interrupts gracefully
        def signal_handler(sig, frame):
            print("\nAnalysis interrupted by user. Exiting...")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        
        analyzer = EFSAnalyzer(
            mount_path=args.mount_path,
            output_dir=args.output_dir,
            exclude_dirs=args.exclude,
            parallel=args.parallel,
            max_depth=args.max_depth
        )
        
        analyzer.analyze()
        text_path, html_path = analyzer.save_reports()
        
        print(f"\nAnalysis complete!")
        print(f"Text report: {text_path}")
        print(f"HTML report: {html_path}")
        print(f"Error log: {error_log}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    # Force the output to be unbuffered for better progress bar display
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
    exit(main())