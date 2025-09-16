#############################
# copyRight Author : CAS-aircas Yisen 
# time : 30/08/2025 Saturday
# written in : Beijing China 
# brief introduce : Weekend unzipping session, simple and clean code works best
# keep writting is significant for a elegant code.
#############################

import os
import sys
import zipfile
import glob
import argparse
from pathlib import Path
from multiprocessing import cpu_count
from joblib import Parallel, delayed


def unzip_S1_SLC(zip_file, target_dir):
    """
    Unzip a single Sentinel-1 SLC zip file
    Args:
        zip_file: Path to the S1 SLC zip file
        target_dir: Target directory to extract files
    Returns:
        bool: True if successful, False otherwise
    """
    zip_path = Path(zip_file)
    target_path = Path(target_dir)
    print(f"Unzipping {zip_path.name} ...")
    
    # Generate SAFE directory name
    safe_name = zip_path.stem + ".SAFE"
    safe_dir = target_path / safe_name
    
    # Skip if already extracted
    if safe_dir.exists():
        print(f"Already exists: {safe_name}, skipping...")
        return True
    
    # Create log file
    log_file = target_path / f'unzip_{zip_path.stem}.log'
    
    try:
        with open(log_file, 'w', encoding='utf-8') as log_f:
            log_f.write(f'unzip_{zip_path.name}:\n')
            
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                total_files = len(file_list)
                
                log_f.write(f'Total files to extract: {total_files}\n')
                log_f.write('Extracting files:\n')
                
                for i, file_name in enumerate(file_list, 1):
                    log_f.write(f'\t[{i:4d}/{total_files}] {file_name}\n')
                    zip_ref.extract(file_name, path=target_dir)
                
                log_f.write('Extraction completed successfully.\n')
        
        print(f"Unzip finished: {zip_path.name}")
        return True
        
    except Exception as e:
        error_msg = f"ERROR during unzipping {zip_path.name}: {str(e)}"
        print(error_msg)
        
        # Log the error
        with open(log_file, 'a', encoding='utf-8') as log_f:
            log_f.write(f"\n{error_msg}\n")
        
        # Clean up partial extraction if exists
        if safe_dir.exists():
            import shutil
            shutil.rmtree(safe_dir, ignore_errors=True)
            print(f"Cleaned up partial extraction: {safe_name}")
        
        return False


def unzip_S1_SLC_list(zip_files, target_dir, n_jobs=None):
    """
    Unzip multiple Sentinel-1 SLC zip files in parallel
    Args:
        zip_files: List of zip file paths
        target_dir: Target directory to extract files
        n_jobs: Number of parallel jobs (None for auto)
    Returns:
        tuple: (successful_count, failed_count)
    """
    target_path = Path(target_dir)
    target_path.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"Starting parallel unzip of {len(zip_files)} S1 SLC files")
    print(f"Target directory: {target_dir}")
    
    # Calculate parallel jobs
    if n_jobs is None:
        n_jobs = min(max(int(cpu_count() / 4), 2), 8)
    print(f"Using {n_jobs} parallel jobs (CPU cores: {cpu_count()})")
    print(f"{'='*60}\n")
    
    # Execute parallel unzipping
    results = Parallel(n_jobs=n_jobs)(
        delayed(unzip_S1_SLC)(zip_file, target_dir) for zip_file in zip_files
    )
    
    # Count results
    successful = sum(results)
    failed = len(results) - successful
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"Unzip Summary:")
    print(f"  Total files: {len(zip_files)}")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Target directory: {target_dir}")
    print(f"{'='*60}")
    
    return successful, failed


def get_S1_zip_files(data_dir):
    """
    Get all Sentinel-1 zip files from data directory
    Args:
        data_dir: Directory containing S1 zip files
    Returns:
        list: List of S1 zip file paths
    """
    data_path = Path(data_dir)
    
    if not data_path.exists():
        raise FileNotFoundError(f"Data directory does not exist: {data_dir}")
    
    if not data_path.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {data_dir}")
    
    # Find all zip files
    zip_pattern = str(data_path / "*.zip")
    zip_files = glob.glob(zip_pattern)
    
    # Filter for S1 files
    s1_zip_files = []
    for zip_file in zip_files:
        zip_name = Path(zip_file).name
        if zip_name.startswith('S1') and '.SAFE' in zip_name:
            s1_zip_files.append(zip_file)
        else:
            print(f"Warning: Skipping non-S1 file: {zip_name}")
    
    return sorted(s1_zip_files)


def create_parser():
    """Create argument parser"""
    parser = argparse.ArgumentParser(
        description='Unzip Sentinel-1 SLC zip files in parallel'
    )
    
    parser.add_argument('--data-dir', type=str, required=True, 
                       help='Directory containing Sentinel-1 zip files')
    parser.add_argument('--slc-dir', type=str, required=True, 
                       help='Target directory to unzip SLC files into')
    parser.add_argument('--job-dir', type=int, default=None, 
                       help='Number of parallel jobs')
    parser.add_argument('--quiet', action='store_true', 
                       help='Suppress detailed output, only show errors and summary')
    
    return parser


if __name__ == "__main__":
    """show the logo od PyIntfLab"""
    init_pysarlab()
    
    """Main processing function"""
    parser = create_parser()
    args = parser.parse_args()
    
    # Get S1 zip files
    zip_files = get_S1_zip_files(args.data_dir)
    
    if not zip_files:
        print(f"No Sentinel-1 zip files found in {args.data_dir}")
        print("Expected file pattern: S1*.SAFE.zip")
        sys.exit(1)
    
    if not args.quiet:
        print(f"Found {len(zip_files)} Sentinel-1 zip files:")
        for i, zip_file in enumerate(zip_files, 1):
            print(f"  {i:2d}. {Path(zip_file).name}")
    
    # Unzip all files
    successful, failed = unzip_S1_SLC_list(zip_files, args.slc_dir, args.jobs)
    
    # Exit with appropriate code
    if failed > 0:
        print(f"\nWarning: {failed} files failed to unzip")
        sys.exit(1)
    else:
        print(f"\nAll files unzipped successfully!")
        sys.exit(0)