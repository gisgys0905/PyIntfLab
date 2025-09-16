#############################
# copyRight Author : CAS-aircas Yisen 
# time : 30/08/2025 Saturday
# written in : Beijing China 
# brief introduce : using stackSentinel.py to generate the runfiles
# I miss the sunset in the Datun road
#############################

import os
import sys
import argparse
import subprocess
from pathlib import Path


def find_dem_file(dem_dir):
    """
    Find DEM file ending with 'wgs84'
    
    Args:
        dem_dir (str): Directory containing DEM files
        
    Returns:
        str: Path to DEM file or None if not found
    """
    dem_path = Path(dem_dir)
    
    if not dem_path.exists():
        raise FileNotFoundError(f"DEM directory does not exist: {dem_dir}")
    
    if not dem_path.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {dem_dir}")
    
    # Look for files ending with 'wgs84'
    dem_files = list(dem_path.glob('*wgs84'))
    
    if not dem_files:
        raise FileNotFoundError(f"No DEM file ending with 'wgs84' found in {dem_dir}")
    
    if len(dem_files) > 1:
        print(f"Warning: Multiple DEM files found, using: {dem_files[0].name}")
    
    return str(dem_files[0])


def stack_sentinel(lat_min, lat_max, lon_min, lon_max, 
                   dem_dir, aux_dir, slc_dir, orbits_dir,
                   nalks, nrlks, process_dir):
    """
    Generate runfiles using stackSentinel.py
    
    Args:
        lat_min, lat_max, lon_min, lon_max (float): Bounding box coordinates
        dem_dir (str): Directory containing DEM files
        aux_dir (str): Directory containing AUX files
        slc_dir (str): Directory containing Sentinel-1 SLC files
        orbits_dir (str): Directory containing orbit files
        nalks (int): Number of azimuth looks
        nrlks (int): Number of range looks
        process_dir (str): Directory where runfiles will be generated
    """
    
    # Change to process directory
    process_path = Path(process_dir)
    process_path.mkdir(parents=True, exist_ok=True)
    
    # Find DEM file
    dem_file = find_dem_file(dem_dir)
    
    # Construct bounding box string
    bbox = f'{lat_min} {lat_max} {lon_min} {lon_max}'
    
    # Build command
    cmd = [
        'stackSentinel.py',
        '-b', bbox,
        '-d', dem_file,
        '-a', aux_dir,
        '-s', slc_dir,
        '-o', orbits_dir,
        '-z', str(nalks),
        '-r', str(nrlks),
        '-f', '0.8',
        '-c', '1'
    ]
    
    print(f"Stack Sentinel parameters:")
    print(f"  Bounding box: [{lat_min}, {lat_max}] x [{lon_min}, {lon_max}]")
    print(f"  DEM file: {dem_file}")
    print(f"  AUX directory: {aux_dir}")
    print(f"  SLC directory: {slc_dir}")
    print(f"  Orbits directory: {orbits_dir}")
    print(f"  Azimuth looks: {nalks}")
    print(f"  Range looks: {nrlks}")
    print(f"  Process directory: {process_dir}")
    print(f"Running command: {' '.join(cmd)}\n")
    
    try:
        result = subprocess.run(
            cmd,
            cwd=process_dir,
            check=True,
            capture_output=True,
            text=True
        )
        
        print("Stack Sentinel completed successfully!")
        if result.stdout:
            print("Output:")
            print(result.stdout)
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"ERROR: stackSentinel.py failed with return code {e.returncode}")
        if e.stdout:
            print("Stdout:", e.stdout)
        if e.stderr:
            print("Stderr:", e.stderr)
        return False
    except FileNotFoundError:
        print("ERROR: stackSentinel.py not found. Please ensure ISCE2 is installed and in PATH.")
        return False
    except Exception as e:
        print(f"ERROR: Unexpected error: {str(e)}")
        return False


def create_parser():
    """Create argument parser"""
    parser = argparse.ArgumentParser(
        description='Generate ISCE2 stack processing runfiles using stackSentinel.py'
    )
    
    # Coordinate arguments
    parser.add_argument('--lat-min', type=float, required=True,
                       help='Minimum latitude of the study area')
    parser.add_argument('--lat-max', type=float, required=True,
                       help='Maximum latitude of the study area')
    parser.add_argument('--lon-min', type=float, required=True,
                       help='Minimum longitude of the study area')
    parser.add_argument('--lon-max', type=float, required=True,
                       help='Maximum longitude of the study area')
    
    # Directory arguments
    parser.add_argument('--dem-dir', type=str, required=True,
                       help='Directory containing DEM files')
    parser.add_argument('--aux-dir', type=str, required=True,
                       help='Directory containing AUX files')
    parser.add_argument('--slc-dir', type=str, required=True,
                       help='Directory containing Sentinel-1 SLC files')
    parser.add_argument('--orbits-dir', type=str, required=True,
                       help='Directory containing orbit files')
    
    # Processing parameters
    parser.add_argument('--nalks', type=int, required=True,
                       help='Number of azimuth looks')
    parser.add_argument('--nrlks', type=int, required=True,
                       help='Number of range looks')
    parser.add_argument('--process-dir', type=str, required=True,
                       help='Directory where runfiles will be generated')
    
    return parser


def validate_coordinates(args):
    """Validate coordinate inputs"""
    if args.lat_min >= args.lat_max:
        print("ERROR: lat_min must be less than lat_max")
        return False
    
    if args.lon_min >= args.lon_max:
        print("ERROR: lon_min must be less than lon_max")
        return False
        
    if not (-90 <= args.lat_min <= 90 and -90 <= args.lat_max <= 90):
        print("ERROR: Latitude values must be between -90 and 90")
        return False
        
    if not (-180 <= args.lon_min <= 180 and -180 <= args.lon_max <= 180):
        print("ERROR: Longitude values must be between -180 and 180")
        return False
    
    return True


def validate_parameters(args):
    """Validate processing parameters"""
    if args.nalks <= 0 or args.nrlks <= 0:
        print("ERROR: nalks and nrlks must be positive integers")
        return False
    
    return True


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    
    # Validate inputs
    if not validate_coordinates(args):
        sys.exit(1)
    
    if not validate_parameters(args):
        sys.exit(1)
    
    # Run stack sentinel
    success = stack_sentinel(
        args.lat_min, args.lat_max, args.lon_min, args.lon_max,
        args.dem_dir, args.aux_dir, args.slc_dir, args.orbits_dir,
        args.nalks, args.nrlks, args.process_dir
    )
    
    sys.exit(0 if success else 1)