#!/usr/bin/env python
import time,sys
import psutil

def bytes2human(n):
    """
    >>> bytes2human(10000)
    '9.8 K'
    >>> bytes2human(100001221)
    '95.4 M'
    """
    symbols = ('K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
    prefix = {}
    for i, s in enumerate(symbols):
        prefix[s] = 1 << (i + 1) * 10
    for s in reversed(symbols):
        if n >= prefix[s]:
            value = float(n) / prefix[s]
            return '%.4f %s' % (value, s)
    return '%.4f B' % (n)
def initargs(interval,):
	while True:
	    try:
		tot_before = psutil.net_io_counters()
		time.sleep(interval)
		tot_after = psutil.net_io_counters()
		diff_recv = tot_after.bytes_recv-tot_before.bytes_recv
		diff_sent = tot_after.bytes_sent-tot_before.bytes_sent
		print "receive : " + str(bytes2human(diff_recv))
		print 'sent : '+ str(bytes2human(diff_sent)) +" in %d second ,speed = " % (interval)+str(bytes2human(diff_sent/interval))
		return diff_sent/interval
	    except (KeyboardInterrupt, SystemExit):
	        break
