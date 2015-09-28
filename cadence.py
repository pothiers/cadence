#! /usr/bin/env python
"""Calculate the statistics of FITS file generation"""

# EXAMPLE:
#  ./cadence.py data/cadence-201411.out 

import sys
import argparse
import logging

import re
from datetime import datetime
from datetime import date
from datetime import time
from datetime import timedelta
from collections import defaultdict
from collections import Counter



def plot_moving_avg(cadence_file, interval=30*60, start_of_day_hour=17):
    '''Plot collected mbits/sec
interval :: average over this interval (seconds)'''
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.dates import DateFormatter

    moving_average = dict() # moving_average[time] = size (mb)
    span = timedelta(seconds=interval)
    #!size, when, st = scrape_hdr_grep(cadence_file)
    files =  scrape_hdr_grep(cadence_file) # dict(fn) = dict(when,where,size)

    timeline = dict() # timeline[(when,where)] = size
    when_set = set()
    where_set = set()
    for fn,dd in files.items():
        timeline[(dd['when'], dd['where'])] = dd['size']
        when_set.add(dd['when'])
        where_set.add(dd['where'])
    
    #!times = sorted(st.keys())
    times = sorted(list(when_set))
    max_mbit_instrum = dict()

    for where in where_set:
        # Initialized moving sum
        msum = 0 # moving sum over interval
        max_mbit = 0

        # find index of last time in interval
        curr = 0
        latest = times[0] + span
        while times[curr] <= latest:
            curr += 1
        
        # find index of first time in interval
        prev = 0
        earliest = times[curr] - span 
        while times[prev] <= earliest:
            prev += 1

        for idx in range(prev,curr):
            msum += timeline.get((times[idx],where),0)

        old_earliest = earliest
        old_prev = prev
        for curr in range(curr+1,len(times)):
            # Find index of first time in new interval
            earliest = times[curr] - span
            while times[prev] <= earliest:
                prev += 1

            # remove obsolete values off start of time window
            for idx in range(old_prev, prev):
                msum -= timeline.get((times[idx],where),0)

            # Add new values to end of time window
            msum += timeline.get((times[curr],where),0)

            moving_average[(times[curr],where)] = msum/interval # mbits/second
            max_mbit = max(msum/interval,max_mbit)
            old_prev = prev
        # END: for curr in range
        max_mbit_instrum[where] = max_mbit
    # END: for where
    print('Max mbits/sec per instrument={}'.format(max_mbit_instrum))

    #!dt_list = sorted(moving_average.keys())
    dt_list = times
    days = set([dt.date() for dt in dt_list])
    print('dt_list cnt={}, day cnt={}, days[0]={}'
          .format(len(dt_list), len(days), list(days)[0]))

    plt.figure(figsize=(8,8))
    plt.hold(True)
    plt.xlabel('Observation time of day')
    plt.ylabel('mbits/sec (collect)')
    plt.ylim(0,18.1)
    formatter = DateFormatter('%H:%M')
    plt.gcf().axes[0].xaxis.set_major_formatter(formatter)  
    max_avg = defaultdict(float) # max_avg[time] = mbits/sec
    clut = dict(zip(where_set,'bgrcmybgrcmybgrcmy'))
    labeled = set()
    for idx,day in enumerate(days):
        dts = [dt for dt in dt_list if dt.date() == day]
        x = [datetime.combine(date(2000,1,1), n.time()) for n in dts]
        for where in where_set:
            y = [moving_average.get((n,where),0) for n in dts]
            #!plt.plot(x,y,label=str(day))
            #!line, = plt.plot(x, y, color=clut[where])
            line, = plt.plot(x, y,  color=clut[where])
            if where not in labeled:
                line.set_label(where)
                labeled.add(where)
            else:
                line.set_label('_'+where)

    #!x = list()
    #!y = list()
    #!for xd in sorted(max_avg.keys()):
    #!    x.append(xd)
    #!    y.append(max_avg[xd])
    #!plt.bar(x,y, label='Max over {} days'.format(len(days)))
    plt.legend(loc='upper center')
    plt.title('Moving average (each of {} days) over interval of {} minutes\n'
              .format(len(days), interval/60))
    plt.savefig('cadence.png')
    plt.show()
    return moving_average


def scrape_hdr_grep(cadence_file):
    """For each file: get observatory, size and date/time generated."""
    re_cad = re.compile(r"(?P<filename>[^:]+):(?P<fldname>#?[\w-]+)\s*=\s*(?P<fldvalue>[^/]+)")

    #!size = dict() # size[filename] = mbytes
    #!when = dict() # when[filename] = datetime generated
    #!where = dict() # where[filename] = observatory
    files = defaultdict(dict) # files[filename] = dict(size,when,where)
    dt_fmt = '%Y-%m-%dT%H:%M:%S'
    t_fmt = '%H:%M:%S'
    unknown_fields = set()
    for line in cadence_file:
        m = re_cad.match(line)
        if not m:
            logging.error('IGNORING: Could not match line: {}'.format(line))
            continue
        filename = m.group('filename')
        telescope = filename.split('/')[6]
        #!print('{} Fieldname="{}"'.format(filename, m.group('fldname')))
        files[filename]['where'] = telescope # if there is no OBSERVAT
        
        if '#filesize' == m.group('fldname'):
            mbytes = int(m.group('fldvalue').strip().split()[0])/(1.0*1e6)
            files[filename]['size'] = mbytes
        #!elif filename in when:
        #!    continue
        elif 'ODATEOBS' == m.group('fldname'):
            continue
        elif 'MJD-OBS' == m.group('fldname'):
            continue
        elif 'DATE-OBS' == m.group('fldname'):
            dtstr = m.group('fldvalue').replace("'","").strip()[:19]
            try:
                dt = datetime.strptime(dtstr, dt_fmt)
            except:
                logging.info(
                    'Ignoring DATE-OBS: Could not parse date/time string={}'
                    .format(dtstr))
                continue
            else:
                files[filename]['when'] = dt
        elif 'TIME-OBS' == m.group('fldname'):
            tstr = m.group('fldvalue').replace("'","").strip()[:8]
            yyyymmdd = filename.split('/')[5]
            dtstr = '{}-{}-{}T{}'.format(yyyymmdd[0:4],
                                         yyyymmdd[4:6],
                                         yyyymmdd[6:8],
                                         tstr)
            try:
                dt = datetime.strptime(dtstr, dt_fmt)
            except:
                logging.info(
                    'Ignoring TIME-OBS: Could not parse date/time string={}'
                    .format(dtstr))
                continue
            else:
                files[filename]['when'] = dt
        elif 'DATE' == m.group('fldname'):
            dtstr = m.group('fldvalue').replace("'","").strip()[:19]
            try:
                dt = datetime.strptime(dtstr, dt_fmt)
            except:
                logging.info(
                    'Ignoring DATE: Could not parse date/time string={}'
                    .format(dtstr))
                continue
            else:
                files[filename]['when'] = dt
#!        elif 'OBSERVAT' == m.group('fldname'):
#!            files[filename]['where'] = (
#!                m.group('fldvalue').replace("'","").strip())
        else:
            unknown_fields.add(m.group('fldname'))
    ###############
    # Done parsing
    #
    #!print('files={}'.format(files))
    prm = dict(filecnt = len(files),
               notwhen  = len([fn for fn in files if 'when' not in files[fn]]),
               notwhere = len([fn for fn in files if 'where' not in files[fn]]),
               notsize  = len([fn for fn in files if 'size' not in files[fn]]),
               )
    prm['whenprc'] =  1.0*prm['notwhen'] / prm['filecnt']
    prm['whereprc'] = 1.0*prm['notwhere'] / prm['filecnt']
    prm['sizeprc']  = 1.0*prm['notsize'] / prm['filecnt']

    print('Files missing WHERE: {}'
          .format([fn for fn in files if 'where' not in files[fn]][:10]))


    print('Missing {notwhen:4d}/{filecnt} ({whenprc:.0%}) time-stamps\n'
          '        {notwhere:4d}/{filecnt} ({whereprc:.0%}) observatories\n'
          '        {notsize:4d}/{filecnt} ({sizeprc:.0%}) sizes'
          .format(**prm))

    
    # Dummy values for missing fields
    maxdt = max([files[fn]['when'] for fn in files if 'when' in files[fn]])
    for fn in files.keys():
        files[fn].setdefault('when', maxdt + timedelta(days=1))
        files[fn].setdefault('where', 'NA')
        files[fn].setdefault('size', )

    locs = Counter([dd['where'] for dd in files.values()])
    print('Observatories: {}'.format(locs.items()))

        
    return files # dict[fn] = dict(when, where, size)
    

##############################################################################

def main():
    "Parse command line arguments and do the work."
    #!print('EXECUTING: %s\n\n' % (' '.join(sys.argv)))
    parser = argparse.ArgumentParser(
        description='My shiny new python program',
        epilog='EXAMPLE: %(prog)s a b"'
        )
    parser.add_argument('--version', action='version', version='1.0.1')
    parser.add_argument('infile', type=argparse.FileType('r'),
                        help='Input file')
    #!parser.add_argument('outfile', type=argparse.FileType('w'),
    #!                    help='Output output')

    parser.add_argument('--loglevel',
                        help='Kind of diagnostic output',
                        choices=['CRTICAL', 'ERROR', 'WARNING',
                                 'INFO', 'DEBUG'],
                        default='WARNING')
    args = parser.parse_args()
    #!args.outfile.close()
    #!args.outfile = args.outfile.name

    #!print 'My args=',args
    #!print 'infile=',args.infile

    log_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(log_level, int):
        parser.error('Invalid log level: %s' % args.loglevel)
    logging.basicConfig(level=log_level,
                        format='%(levelname)s %(message)s',
                        datefmt='%m-%d %H:%M')
    logging.debug('Debug output is enabled in %s !!!', sys.argv[0])

    #!scrape_hdr_grep(args.infile)
    plot_moving_avg(args.infile)

if __name__ == '__main__':
    main()
