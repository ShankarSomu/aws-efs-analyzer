#!/usr/bin/env python3
"""
EFS Analyzer - Analyzes EFS mount points for cost optimization opportunities

This script scans an EFS mount point, categorizes files based on last access time,
and calculates potential cost savings by moving data to different storage tiers.
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict

# Global logger
logger = None

def setup_logging(log_file):
    logger = logging.getLogger("directory_scanner")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger

def log_error(message, logger):
    logger.error(message)

def scan_directory(directory, exclude_dirs, current_time, max_depth, current_depth,
                   parallel, follow_symlinks, system_dirs, visited_paths, error_log_path=None):
    global logger
    file_data = []
    subdirs = []

    try:
        for entry in os.scandir(directory):
            try:
                entry_path = Path(entry.path)

                if entry.is_symlink() and not follow_symlinks:
                    continue

                real_path = entry_path.resolve()
                if real_path in visited_paths:
                    continue
                visited_paths.add(real_path)

                if entry.is_file(follow_symlinks=follow_symlinks):
                    try:
                        stat = entry.stat(follow_symlinks=follow_symlinks)
                        last_modified = datetime.fromtimestamp(stat.st_mtime)
                        size = stat.st_size

                        if last_modified < current_time - timedelta(days=180):
                            if size < 1024:
                                category = 'Category A'
                            elif size < 1024 * 1024:
                                category = 'Category B'
                            elif size < 1024 * 1024 * 1024:
                                category = 'Category C'
                            else:
                                category = 'Category D'
                            file_data.append((category, size))
                    except Exception as e:
                        log_error(f"Error accessing file {entry.path}: {e}", logger)

                elif entry.is_dir(follow_symlinks=follow_symlinks):
                    subdir_path = entry_path
                    if subdir_path.name not in exclude_dirs and not any(
                        str(subdir_path).startswith(str(sys_dir)) for sys_dir in system_dirs
                    ):
                        subdirs.append(subdir_path)

            except Exception as e:
                log_error(f"Error processing entry {entry.path}: {e}", logger)

    except Exception as e:
        log_error(f"Error scanning directory {directory}: {e}", logger)

    if current_depth < max_depth and subdirs:
        if parallel > 1:
            with ProcessPoolExecutor(max_workers=parallel) as executor:
                subdir_args = [
                    (subdir, exclude_dirs, current_time, max_depth, current_depth + 1,
                     max(1, parallel // len(subdirs)), error_log_path, follow_symlinks, system_dirs, visited_paths)
                    for subdir in subdirs
                ]
                futures = [executor.submit(process_subdirectory_safe, args) for args in subdir_args]
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        file_data.extend(result)
                    except Exception as e:
                        log_error(f"Error processing subdirectory: {e}", logger)
        else:
            for subdir in subdirs:
                result = scan_directory(subdir, exclude_dirs, current_time, max_depth,
                                        current_depth + 1, parallel, follow_symlinks,
                                        system_dirs, visited_paths, error_log_path)
                file_data.extend(result)

    return file_data

def process_subdirectory(args):
    (subdir, exclude_dirs, current_time, max_depth, current_depth, parallel,
     error_log, follow_symlinks, system_dirs, visited_paths) = args

    worker_logger = setup_logging(error_log)
    global logger
    logger = worker_logger

    return scan_directory(subdir, exclude_dirs, current_time, max_depth, current_depth,
                          parallel, follow_symlinks, system_dirs, visited_paths, error_log)

def process_subdirectory_safe(args):
    try:
        return process_subdirectory(args)
    except Exception as e:
        print(f"Unhandled exception in process_subdirectory_safe: {e}")
        return []

def print_summary(file_data):
    totals = defaultdict(int)
    for category, size in file_data:
        totals[category] += size
    for category in sorted(totals.keys()):
        size_in_mb = totals[category] / (1024 * 1024)
        print(f"{category}: {size_in_mb:.2f} MB")

def main():
    parser = argparse.ArgumentParser(description="Scan a directory and summarize file sizes by category.")
    parser.add_argument("directory", help="Directory to scan")
    parser.add_argument("--exclude", nargs='*', default=['proc', 'sys', 'dev', 'run', 'tmp', 'mnt', 'media'],
                        help="Directories to exclude")
    parser.add_argument("--max-depth", type=int, default=10, help="Maximum recursion depth")
    parser.add_argument("--parallel", type=int, default=4, help="Number of parallel workers")
    parser.add_argument("--follow-symlinks", action="store_true", help="Follow symbolic links")
    parser.add_argument("--error-log", default="scan_errors.log", help="File to log errors")

    args = parser.parse_args()

    global logger
    logger = setup_logging(args.error_log)

    system_dirs = [Path("/proc"), Path("/sys"), Path("/dev"), Path("/run"),
                   Path("/tmp"), Path("/mnt"), Path("/media")]
    current_time = datetime.now()
    visited_paths = set()

    file_data = scan_directory(
        Path(args.directory),
        args.exclude,
        current_time,
        args.max_depth,
        0,
        args.parallel,
        args.follow_symlinks,
        system_dirs,
        visited_paths,
        args.error_log
    )

    print_summary(file_data)

if __name__ == "__main__":
    main()
