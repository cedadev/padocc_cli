__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import os
import argparse
import logging

from padocc import Configuration
from padocc.core import BypassSwitch
from padocc.core.utils import get_attribute, times
from padocc.phases import KNOWN_PHASES

def deploy_array_job( 
        phase     : str,
        groupID   : str, 
        workdir   : str,
        groupdir  : str,
        source    : str,
        venvpath  : str,
        group_len : int,
        logger    : logging.Logger,
        forceful   : bool = None,
        dryrun     : bool = None,
        quality    : bool = None,
        verbose    : int = 0,
        binpack    : bool = None,
        time_allowed : str = None,
        memory       : str = None,
        subset       : int = None,
        repeat_id    : str = 'main',
        bypass       : str = 'FDSC',
        mode         : str = 'kerchunk',
        new_version  : str = None,
        time     : str = None,
        joblabel : str = None,
    ) -> None:
    """
    Configure a single array job for deployment.

    :param args:        (obj) Set of command line arguments supplied by argparse.

    :param logger:      (obj) Logging object for message logging.

    :param time:        (str) Time specified by the current allocation/band

    :param label:       (str) Label to apply to the current allocation/band

    :param group_len:   (int) Integer size of allocation/band group.

    :returns: None
    """

    if not joblabel:
        group_phase_sbatch = f'{groupdir}/sbatch/{phase}.sbatch'
    else:
        group_phase_sbatch = f'{groupdir}/sbatch/{phase}_{joblabel}.sbatch'
        repeat_id = f'{repeat_id}/{joblabel}'

    master_script      = f'{source}/single_run.py'
    template           = 'extensions/templates/phase.sbatch.template'

    # Open sbatch template from file.
    with open(template) as f:
        sbatch = '\n'.join([r.strip() for r in f.readlines()])

    # Setup time and memory defaults
    if time is None:
        time = time_allowed or times[phase]
    mem = '2G' or memory

    jobname = f'{groupID}_{phase}'
    if joblabel:
        jobname = f'{joblabel}_{phase}_{groupID}'

    # Out/Errs
    outdir = '/dev/null'
    errdir = f'{groupdir}/errs/{phase}_{repeat_id.replace("/","_")}/'

    if os.path.isdir(errdir):
        os.system(f'rm -rf {errdir}')
    os.makedirs(errdir)

    errdir = errdir + '%A_%a.log'

    sb = sbatch.format(
        jobname,                              # Job name
        time,                                 # Time
        mem,                                  # Memory
        outdir, errdir,
        venvpath,
        workdir,
        groupdir,
        master_script, phase, groupID, time, mem, repeat_id
    )

    # Additional carry-through flags
    sb += f' -b {bypass}'
    if forceful is not None:
        sb += ' -f'
    verb = 'v' * len(verbose) 
    if verb != '':
        sb += f' -{verb}'
    if quality is not None:
        sb += ' -Q'
    if dryrun is not None:
        sb += ' -d'
    if binpack is not None:
        sb += ' -A'

    if subset is not None:
        sb += f' -s {subset}'
    if new_version is not None:
        sb += f' -n {new_version}'
    if mode is not None:
        sb += f' -m {mode}'

    with open(group_phase_sbatch,'w') as f:
        f.write(sb)

    # Submit job array for this group in this phase
    if dryrun:
        logger.info('DRYRUN: sbatch command: ')
        print(f'sbatch --array=0-{group_len-1} {group_phase_sbatch}')
    else:
        os.system(f'sbatch --array=0-{group_len-1} {group_phase_sbatch}')

def main(
        phase     : str,
        groupID   : str, 
        workdir   : str = None,
        source    : str = None,
        venvpath  : str = None,
        input_file : str = None,
        band_increase : str = None,
        forceful   : bool = None,
        dryrun     : bool = None,
        quality    : bool = None,
        verbose    : int = 0,
        binpack    : bool = None,
        time_allowed : str = None,
        memory       : str = None,
        subset       : int = None,
        repeat_id    : str = 'main',
        bypass       : BypassSwitch = BypassSwitch(),
        mode         : str = 'kerchunk',
        new_version  : str = None,
    ) -> None:
    """
    Assemble sbatch script for parallel running jobs and execute. May include
    allocation of multiple tasks to each job if enabled.

    :param args:    (Object) ArgParse object containing all required parameters
                    from default values or specific inputs from command-line.
    
    :returns: None
    """

    # kwargs: forceful, dryrun, quality, bypass, verbose

    workdir  = get_attribute('WORKDIR', workdir)
    source   = get_attribute('SRCDIR', source)
    venvpath = get_attribute('KVENV', venvpath)

    conf = Configuration(
        groupID, workdir=workdir, label='main-group', 
        fh=None, logid=None, forceful=forceful, dryrun=dryrun,
        quality=quality, bypass=bypass, verbose=verbose, 
    )

    if phase not in KNOWN_PHASES:
        conf.logger.error(
            f'"{phase}" not recognised, please select from {KNOWN_PHASES}'
        )
        return None

    conf.info({
        'Source': source,
        'Venv': venvpath
    })

    # Init not parallelised - run for whole group here
    if phase == 'init':
        conf.logger.info(f'Running init steps as a serial process for {groupID}')
        conf.init_config(input_file)
        return None
    
    array_job_kwargs = {
        'forceful': forceful,
        'dryrun'  : dryrun,
        'quality' : quality,
        'verbose' : verbose,
        'binpack' : binpack,
        'time_allowed' : time_allowed,
        'memory'  : memory,
        'subset'  : subset,
        'repeat_id' : repeat_id,
        'bypass' : bypass,
        'mode' : mode,
        'new_version' : new_version,
    }

    if not time_allowed:        

        allocations = conf.create_allocations(
            phase, repeat_id,
            band_increase=band_increase, binpack=binpack
        )

        for alloc in allocations:
            print(f'{alloc[0]}: ({alloc[1]}) - {alloc[2]} Jobs')

        deploy = input('Deploy the above allocated dataset jobs with these timings? (Y/N) ')
        if deploy != 'Y':
            raise KeyboardInterrupt

        for alloc in allocations:
            deploy_array_job(
                phase, groupID, workdir, conf.dir,
                source, venvpath, alloc[2], conf.logger,
                **array_job_kwargs,
            )
    else:
        num_datasets = len(conf.proj_codes[repeat_id].get())
        conf.logger.info(f'All Datasets: {time_allowed} ({num_datasets})')

        # Always check before deploying a significant number of jobs.
        deploy = input('Deploy the above allocated dataset jobs with these timings? (Y/N) ')
        if deploy != 'Y':
            raise KeyboardInterrupt

        deploy_array_job(
                phase, groupID, workdir, conf.dir,
                source, venvpath, num_datasets, conf.logger,
                **array_job_kwargs,
            )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a pipeline step for a group of datasets')
    parser.add_argument('phase', type=str, help='Phase of the pipeline to initiate')
    parser.add_argument('groupID',type=str, help='Group identifier code')

    # Group-run specific
    parser.add_argument('-S','--source', dest='source', help='Path to directory containing master scripts (this one)')
    parser.add_argument('-e','--environ',dest='venvpath', help='Path to virtual (e)nvironment (excludes /bin/activate)')
    parser.add_argument('-i', '--input', dest='input', help='input file (for init phase)')
    parser.add_argument('-A', '--alloc-bins', dest='binpack',action='store_true', help='input file (for init phase)')
    parser.add_argument('--allow-band-increase', dest='band_increase',action='store_true', help='Allow automatic banding increase relative to previous runs.')

    # Action-based - standard flags
    parser.add_argument('-f','--forceful',dest='forceful',action='store_true', help='Force overwrite of steps if previously done')
    parser.add_argument('-v','--verbose', dest='verbose', action='count', default=0, help='Print helpful statements while running')
    parser.add_argument('-d','--dryrun',  dest='dryrun',  action='store_true', help='Perform dry-run (i.e no new files/dirs created)' )
    parser.add_argument('-Q','--quality', dest='quality', action='store_true', help='Quality assured checks - thorough run')
    parser.add_argument('-b','--bypass-errs', dest='bypass', default='DBSCL', help=BypassSwitch().help())
    parser.add_argument('-B','--backtrack', dest='backtrack', action='store_true', help='Backtrack to previous position, remove files that would be created in this job.')

    # Environment variables
    parser.add_argument('-w','--workdir',   dest='workdir',      help='Working directory for pipeline')

    # Single-job within group
    parser.add_argument('-G','--groupID',   dest='groupID', default=None, help='Group identifier label')
    parser.add_argument('-t','--time-allowed',dest='time_allowed',  help='Time limit for this job')
    parser.add_argument('-M','--memory', dest='memory', default='2G', help='Memory allocation for this job (i.e "2G" for 2GB)')
    parser.add_argument('-s','--subset',    dest='subset',    default=1,   type=int, help='Size of subset within group')
    parser.add_argument('-r','--repeat_id', dest='repeat_id', default='main', help='Repeat id (main if first time running, <phase>_<repeat> otherwise)')

    # Specialised
    parser.add_argument('-n','--new_version', dest='new_version',   help='If present, create a new version')
    parser.add_argument('-m','--mode',        dest='mode', default=None, help='Output format mode: kerchunk, zarr etc.')

    args = parser.parse_args()

    main(
        args.phase, args.groupID,
        workdir=args.workdir, source=args.source, venvpath=args.venvpath, input_file=args.input,
        band_increase=args.band_increase,
        forceful=args.forceful, verbose=args.verbose, dryrun=args.dryrun, quality=args.quality,
        binpack=args.binpack, time_allowed=args.time_allowed, memory=args.memory, subset=args.subset,
        repeat_id=args.repeat_id, bypass=args.bypass, new_version=args.new_version, mode=args.mode
    )

    