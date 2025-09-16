#############################
# copyRight Author : CAS-aircas Yisen 
# time : 30/08/2025 Saturday
# written in : Beijing China 
# brief introduce : dowload the S1 orbit files using requests
# sunny day in Beijing at 01/09/2025
#############################

import os
import sys
import glob
import requests
import argparse
from datetime import datetime, timedelta
from multiprocessing import cpu_count
from joblib import Parallel, delayed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

global ORBIT_URL 
ORBIT_URL = "https://s1qc.asf.alaska.edu/aux_poeorb/"

def create_session():
    """
    Create requests session to connect the poeorb
    """
    session = requests.Session()
    retries = Retry(
        total=5,                
        backoff_factor=1,      
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    return session


def download_file(session, task):
    """
    download the orbit files 
    Args:
        session (requests.session): the session between user and net
        task (list): the task of download files 
    """
    file_url, save_path = task
    try:
        with session.get(file_url, stream=True, timeout=666) as r:
            r.raise_for_status()
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print(f"Downloaded: {os.path.basename(save_path)}")
    except Exception as e:
        print(f"Failed to download {file_url}: {e}")


def downloda_S1_SLC_orbit_list(zipped_dir, orbits_dir):
    """
    download S1*.zip orbit files 
    Args:
        zipped_dir (str)   :  the zipped directory of the S1*.zip 
        orbits_dir (str)   :  the orbit files output directory 
    """
    # get the zipped file list
    session = create_session()
    resp = session.get(ORBIT_URL, timeout=666).content
    lst = [str(i[:77])[2:-1] for i in resp.split(b'href="')]
    download_tasks = []
    S1A_dir = os.path.join(zipped_dir, 'S1A*.zip')
    for file in glob.glob(S1A_dir):
        date_str = file.split('_')[-5][:8]
        y, m, d = int(date_str[:4]),  int(date_str[4:6]),  int(date_str[6:8])
        dt = datetime(y, m, d)
        prev_dt = dt - timedelta(days=1)
        next_dt = dt + timedelta(days=1)
        prev_dt_str = f'{prev_dt.year}{prev_dt.month:02d}{prev_dt.day:02d}'
        next_dt_str = f'{next_dt.year}{next_dt.month:02d}{next_dt.day:02d}'
        for filename in lst:
            if (filename[-35:-27] == prev_dt_str) & (filename[-19:-11] == next_dt_str):
                download_tasks.append((
                    f"{ORBIT_URL}{filename}",
                    os.path.join(orbits_dir, filename)
                                    ))
                          
    njobs = min(max(cpu_count() // 4, 2), 8)
    print(f"Starting parallel download with {njobs} jobs...")
    Parallel(n_jobs=njobs)(
        delayed(download_file)(session, task) for task in download_tasks
    )
    
    print("All downloads finished.")


def create_parser():
    """Create argument parser"""
    parser = argparse.ArgumentParser(
        description='Download Sentinel-1 SLC orbit files'
    )
    
    parser.add_argument('--zipped-dir', type=str, required=True,
                       help='Directory containing Sentinel-1 zip files')
    parser.add_argument('--orbit-dir', type=str, required=True,
                       help='Output directory for orbit files')
    
    return parser


if __name__ == '__main__':
    """main function"""
    parser = create_parser()
    args = parser.parse_args()

    downloda_S1_SLC_orbit_list(args.zipped_dir, args.orbit_dir)