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

# ssh dsan3
# sudo su -
# egrep -f /home/pothiers/cadence/patterns.dat /net/mss1/archive/mtn/2014123?/*/*/*.hdr > /home/pothiers/cadence/cadence.out


def plot_moving_avg(cadence_file, interval=30*60, start_of_day_hour=17):
    '''Plot collected mbits/sec
interval :: average over this interval (seconds)'''
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.dates import DateFormatter

    moving_average = dict() # moving_average[time] = size (mb)
    span = timedelta(seconds=interval)
    size, when, st = scrape_hdr_grep(cadence_file)
    times = sorted(st.keys())
    latest = times[0] + span

    curr = 0
    while times[curr] <= latest:
        curr += 1

    earliest = times[curr] - span
    prev = 0
    msum = 0 # moving sum over interval
    while times[prev] <= earliest:
        prev += 1

    # Initialized moving sum
    for idx in range(prev,curr):
        msum += st[times[idx]]

    old_earliest = earliest
    old_prev = prev
    for curr in range(curr+1,len(times)):
        earliest = times[curr] - span
        # Find idx of start of new interval
        while times[prev] <= earliest:
            prev += 1

        # remove obsolete values
        for idx in range(old_prev, prev):
            msum -= st[times[idx]]

        msum += st[times[curr]]
        moving_average[times[curr]] = msum/interval # mbits per second
        old_earliest = earliest
        old_prev = prev

    dt_list = sorted(moving_average.keys())
    days = set([dt.date() for dt in dt_list])
    print('dt_list cnt={}, day cnt={}, days[0]={}'
          .format(len(dt_list), len(days), list(days)[0]))

    plt.figure(figsize=(8,5))
    plt.hold(True)
    plt.xlabel('Observation time of day')
    plt.ylabel('mbits/sec (collect)')
    formatter = DateFormatter('%H:%M')
    plt.gcf().axes[0].xaxis.set_major_formatter(formatter)  
    max_avg = defaultdict(float) # max_avg[time] = mbits/sec
    for day in days:
        dts = [dt for dt in dt_list if dt.date() == day]
        x = [datetime.combine(date(2000,1,1), n.time()) for n in dts]
        y = [moving_average[n] for n in dts]
        plt.plot(x,y,label=str(day))
        #!for idx in range(len(x)):
        #!    max_avg[x[idx]] = max(max_avg[x[idx]], y[idx])

    #!x = list()
    #!y = list()
    #!for xd in sorted(max_avg.keys()):
    #!    x.append(xd)
    #!    y.append(max_avg[xd])
    #!plt.bar(x,y, label='Max over {} days'.format(len(days)))
    #!plt.legend()
    plt.title('Moving average (each of {} days) over interval of {} minutes\n'
              .format(len(days), interval/60))
    plt.savefig('cadence.pdf', dpi=800)
    plt.show()
    return moving_average


def scrape_hdr_grep(cadence_file):
    """For each file: get size and date/time generated."""
    re_cad = re.compile(r"(?P<filename>[^:]+):(?P<fldname>#?[\w-]+)\s*=\s*(?P<fldvalue>[^/]+)")
    size = dict() # size[filename] = mbytes
    when = dict() # when[filename] = datetime generated
    dt_fmt = '%Y-%m-%dT%H:%M:%S'
    t_fmt = '%H:%M:%S'
    unknown_fields = set()
    for line in cadence_file:
        m = re_cad.match(line)
        if not m:
            logging.error('Could not match line: {}'.format(line))
        filename = m.group('filename')
        if '#filesize' == m.group('fldname'):
            mbytes = int(m.group('fldvalue').strip().split()[0])/1e6
            size[filename] = mbytes
        elif filename in when:
            continue
        elif 'ODATEOBS' == m.group('fldname'):
            continue
        elif 'MJD-OBS' == m.group('fldname'):
            continue
        elif 'DATE-OBS' == m.group('fldname'):
            dtstr = m.group('fldvalue').strip().replace("'","")[:19]
            #logging.debug('DATE-OBS={}'.format(dtstr))
            try:
                dt = datetime.strptime(dtstr, dt_fmt)
            except:
                #! logging.error('DATE-OBS: Could not parse date/time string={}'.format(dtstr))
                continue
            else:
                when[filename] = dt
        elif 'TIME-OBS' == m.group('fldname'):
            tstr = m.group('fldvalue').strip().replace("'","")[:8]
            yyyymmdd = filename.split('/')[5]
            dtstr = '{}-{}-{}T{}'.format(yyyymmdd[0:4],
                                         yyyymmdd[4:6],
                                         yyyymmdd[6:8],
                                         tstr)
            try:
                dt = datetime.strptime(dtstr, dt_fmt)
            except:
                #! logging.error('TIME-OBS: Could not parse date/time string={}'.format(dtstr))
                continue
            else:
                when[filename] = dt
        elif 'DATE' == m.group('fldname'):
            dtstr = m.group('fldvalue').strip().replace("'","")[:19]
            #logging.debug('DATE={}'.format(dtstr))
            try:
                dt = datetime.strptime(dtstr, dt_fmt)
            except:
                #! logging.error('DATE: Could not parse date/time string={}'.format(dtstr))
                continue
            else:
                when[filename] = dt
        else:
            unknown_fields.add(m.group('fldname'))
    
    #!print('Unknown FIELDS: {}'.format(unknown_fields))
    fname = list(size.keys())[0]
    #!print('size({}) [{}]={}'.format(len(size), fname, size[fname]))
    #!print('when({}) [{}]={}'.format(len(when), fname, when[fname]))
    missing = set(size) - set(when)
    #!print('\nMissing timestamp for files: {}\n'.format(' '.join(missing)))
    print('Could not calculate {}/{} ({:.1%}) collection time-stamps'
          .format(len(missing),
                  len(size),
                  len(missing)/float(len(size)),
              ))

    st = defaultdict(int) # st[time] = size
    for fname in size.keys():
        if fname not in when:
            continue
        st[when[fname]] += size[fname]
        size,when,st
    return size,when,st
    

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
