#############################
# copyRight Author : CAS-aircas Yisen 
# time : 30/08/2025 Saturday
# written in : Beijing China 
# brief introduce : using dem.py to download Digital Elevation model, exp is .wgs84
# sunny day in Beijing at 01/09/2025
#############################

import os
import sys
import argparse
import subprocess
from pathlib import Path


def download_S1_SLC_dem(lat_min, lat_max, lon_min, lon_max, dem_dir):
    """
    Download Digital Elevation Model using dem.py
    
    Args:
        lat_min (float): Minimum latitude (South)
        lat_max (float): Maximum latitude (North)  
        lon_min (float): Minimum longitude (West)
        lon_max (float): Maximum longitude (East)
        dem_dir (str): Output directory for DEM files
    """
    
    # Create DEM folder
    dem_path = Path(dem_dir)
    dem_path.mkdir(parents=True, exist_ok=True)
    os.chdir(dem_path)
    # Calculate extended boundaries (add 1 degree buffer)
    lat_min_dem = int(float(lat_min)) - 1
    lat_max_dem = int(float(lat_max)) + 1
    lon_min_dem = int(float(lon_min)) - 1
    lon_max_dem = int(float(lon_max)) + 1
    
    # Construct command
    cmd = [
        'dem.py',
        '-a', 'stitch',
        '-b', str(lat_min_dem), str(lat_max_dem), str(lon_min_dem), str(lon_max_dem),
        '-r',
        '-s', '1',
        '-c'
    ]
    
    log_file = dem_path / 'dem.log'
    
    try:
        # Change to DEM directory and run command
        with open(log_file, 'w') as log_f:
            result = subprocess.run(
                cmd, 
                cwd=dem_dir,
                stdout=log_f,
                stderr=subprocess.STDOUT,
                check=True
            )
        
        print("DEM download completed successfully!")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"ERROR: DEM download failed with return code {e.returncode}")
        print(f"Check log file for details: {log_file}")
        return False
    except FileNotFoundError:
        print("ERROR: dem.py not found. Please ensure it's installed and in PATH.")
        return False
    except Exception as e:
        print(f"ERROR: Unexpected error during DEM download: {str(e)}")
        return False


def create_parser():
    """Create argument parser"""
    parser = argparse.ArgumentParser(
        description='Download Digital Elevation Model using dem.py'
    )
    
    parser.add_argument('--lat-min', type=float, required=True,
                       help='Minimum latitude (South boundary)')
    parser.add_argument('--lat-max', type=float, required=True,
                       help='Maximum latitude (North boundary)')
    parser.add_argument('--lon-min', type=float, required=True,
                       help='Minimum longitude (West boundary)')
    parser.add_argument('--lon-max', type=float, required=True,
                       help='Maximum longitude (East boundary)')
    parser.add_argument('--dem-dir', type=str, required=True,
                       help='Output directory for DEM files')
    
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


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    
    # Validate coordinates
    if not validate_coordinates(args):
        sys.exit(1)
    
    # Download DEM
    success = download_S1_SLC_dem(
        args.lat_min, 
        args.lat_max, 
        args.lon_min, 
        args.lon_max, 
        args.dem_dir
    )
    
    sys.exit(0 if success else 1)