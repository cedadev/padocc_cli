
__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import sys
import argparse
import os
import json
import logging
from datetime import datetime
import traceback
import re

# Pipeline Modules
from padocc import Configuration
from padocc.core import BypassSwitch

def main(
        phase     : str,
        proj_code : str, 
        workdir   : str = None,
        groupID   : str = None,
        verbose   : int = 0,
        repeat_id    : str = 'main',
        bypass       : BypassSwitch = BypassSwitch(),
        mode         : str = 'kerchunk',
        **kwargs
    ) -> None:

    """
    Main function for processing a single job. This could be multiple tasks/datasets within 
    a single job, but everything from here is serialised, i.e run one after another.

    :param args:        (obj) Set of command line arguments supplied by argparse.

    :returns: None
    """

    logid = ''

    if os.getenv('SLURM_ARRAY_JOB_ID'):
        jobid = os.getenv('SLURM_ARRAY_JOB_ID')
        taskid = os.getenv('SLURM_ARRAY_TASK_ID')

        logid = f'{jobid}-{taskid}'

    if groupID is None:
        raise NotImplementedError

    bypass = BypassSwitch(switch=bypass)

    conf = Configuration(workdir, groupID=groupID, logger=None, logid=logid, label='main', verbose=verbose)
    conf.check_writable()

    conf.run_group(proj_code=proj_code, mode=mode, repeat_id=repeat_id, phase=phase, **kwargs)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a pipeline step for a single dataset')
    parser.add_argument('phase',    type=str, help='Phase of the pipeline to initiate')
    parser.add_argument('proj_code',type=str, help='Project identifier code')

    # Environment variables
    parser.add_argument('-w','--workdir',   dest='workdir',      help='Working directory for pipeline')
    parser.add_argument('-G','--groupID',      dest='groupID',     default=None,       help='Group identifier label')

    # Action-based - standard flags
    parser.add_argument('-f','--forceful',dest='forceful',      action='store_true', help='Force overwrite of steps if previously done')
    parser.add_argument('-v','--verbose', dest='verbose',       action='count', default=0, help='Print helpful statements while running')
    parser.add_argument('-d','--dryrun',  dest='dryrun',        action='store_true', help='Perform dry-run (i.e no new files/dirs created)' )
    parser.add_argument('-Q','--quality', dest='quality',       action='store_true', help='Create refs from scratch (no loading), use all NetCDF files in validation')
    parser.add_argument('-A', '--alloc-bins', dest='binpack',   action='store_true', help='Use binpacking for allocations (otherwise will use banding)')

    # Single job within group
    parser.add_argument('-t','--time-allowed', dest='time_allowed',help='Time limit for this job')
    parser.add_argument('-M','--memory',    dest='memory',         default='2G',       help='Memory allocation for this job (i.e "2G" for 2GB)')
    parser.add_argument('-s','--subset',    dest='subset',         default=1,type=int, help='Size of subset within group')
    parser.add_argument('-r','--repeat_id', dest='repeat_id',      default='main',     help='Repeat id (1 if first time running, <phase>_<repeat> otherwise)')

    # Specialised
    parser.add_argument('-b','--bypass-errs',   dest='bypass', default='DBSCLR', help=BypassSwitch().help())
    parser.add_argument('-n','--new_version',   dest='new_version',              help='If present, create a new version')
    parser.add_argument('-m','--mode',          dest='mode',   default='kerchunk',     help='Output format mode: (kerchunk, zarr etc.)')
    
    args = parser.parse_args()

    main(
        args.phase, args.proj_code,
        workdir=args.workdir, groupID=args.groupID, 
        forceful=args.forceful, verbose=args.verbose, dryrun=args.dryrun, quality=args.quality,
        binpack=args.binpack, time_allowed=args.time_allowed, memory=args.memory, subset=args.subset,
        repeat_id=args.repeat_id, bypass=args.bypass, new_version=args.new_version, mode=args.mode
    )

    