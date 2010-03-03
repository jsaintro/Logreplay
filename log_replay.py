#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et
# $Id: retriever-multi.py,v 1.29 2005/07/28 11:04:13 mfx Exp $

#
# Usage: python retriever-multi.py <file with URLs to fetch> [<# of
#          concurrent connections>]
#

import sys
import pycurl
import re
import logging
import time
import datetime

LOG_RE = re.compile("""
    ^(?P<year>\d{4})\-(?P<month>\d{2})\-(?P<day>\d{2})\s+
    (?P<hour>\d{2})\:(?P<minute>\d{2})\:(?P<second>\d{2})\s+
    (?P<c_ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+
    (?P<cs_username>.+?)\s+
    (?P<s_ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+
    (?P<s_port>\d{1,3})\s+
    (?P<cs_method>.+?)\s+
    (?P<cs_uri_stem>.+?)\s+
    (?P<cs_uri_query>.+?)\s+
    (?P<sc_status>\d{1,3})\s+
    (?P<sc_bytes>\d+)\s+
    (?P<cs_bytes>\d+)\s+
    (?P<csUser_Agent>.+?)\s+
    (?P<csReferer>.+)
""".strip(), re.IGNORECASE and re.VERBOSE)

filename = "/dev/null"

# We should ignore SIGPIPE when using pycurl.NOSIGNAL - see
# the libcurl tutorial for more info.
try:
    import signal
    from signal import SIGPIPE, SIG_IGN
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except ImportError:
    pass


# Get args
num_conn = 10
try:
    f = file(sys.argv[1])
    server = sys.argv[2]
    if len(sys.argv) >= 4:
        num_conn = int(sys.argv[3])
except:
    print "Usage: %s <file with URLs to fetch> [<# of concurrent connections>]" % sys.argv[0]
    raise SystemExit

# Check args
assert 1 <= num_conn <= 10000, "invalid number of concurrent connections"
print "PycURL %s (compiled against 0x%x)" % (pycurl.version, pycurl.COMPILE_LIBCURL_VERSION_NUM)
print "----- Getting URLs using", num_conn, "connections -----"


# Pre-allocate a list of curl objects
m = pycurl.CurlMulti()
m.handles = []
for i in range(num_conn):
    c = pycurl.Curl()
    c.fp = None
    c.setopt(pycurl.FOLLOWLOCATION, 1)
    c.setopt(pycurl.MAXREDIRS, 5)
    c.setopt(pycurl.CONNECTTIMEOUT, 30)
    c.setopt(pycurl.TIMEOUT, 300)
    c.setopt(pycurl.NOSIGNAL, 1)
    m.handles.append(c)


# Main loop
freelist = m.handles[:]
num_processed = 0
EOF = False
ot = None
slt = None

while not EOF:
    # If there is an url to process and a free curl object, add to multi stack
    while freelist:
        l = f.readline()
        if l == '':
             EOF = True
             break
        pline = LOG_RE.match(l)
        if not pline:
             print "Skipping noise line %s" % l
             continue

        l_time = datetime.datetime(
            int(pline.group("year")),
            int(pline.group("month")),
            int(pline.group("day")),
            int(pline.group("hour")),
            int(pline.group("minute")),
            int(pline.group("second"))
        )

        lt = time.mktime(l_time.timetuple())

        if not ot:
            ot = time.time() - lt
            slt = lt
            off_time = datetime.datetime.now() - l_time

        # Magic algorithm to allow log replay accelleration
        lt = (lt - slt)/2 + slt
        dt = lt - (time.time()-ot)
        if dt < -10:
            print "%d seconds behind schedule" % dt

        url = server + pline.group("cs_uri_stem")
        c = freelist.pop()
        c.fp = open(filename, "wb")
        c.setopt(pycurl.URL, url)
        c.setopt(pycurl.WRITEDATA, c.fp)
        m.add_handle(c)
        # store some info
        c.filename = filename
        c.url = url
    # Run the internal curl state machine for the multi stack
    while 1:
        ret, num_handles = m.perform()
        if ret != pycurl.E_CALL_MULTI_PERFORM:
            break
    # Check for curl objects which have terminated, and add them to the freelist
    while 1:
        num_q, ok_list, err_list = m.info_read()
        for c in ok_list:
            c.fp.close()
            c.fp = None
            m.remove_handle(c)
            print "Success:", c.filename, c.url, c.getinfo(pycurl.EFFECTIVE_URL)
            freelist.append(c)
        for c, errno, errmsg in err_list:
            c.fp.close()
            c.fp = None
            m.remove_handle(c)
            print "Failed: ", c.filename, c.url, errno, errmsg
            freelist.append(c)
        num_processed = num_processed + len(ok_list) + len(err_list)
        if num_q == 0:
            break
    # Currently no more I/O is pending, could do something in the meantime
    # (display a progress bar, etc.).
    # We just call select() to sleep until some more data is available.
    print "Log Time: ",l_time," Current time: ",datetime.datetime.now()-off_time,dt
    if dt > 0:
       time.sleep(dt)
       print "Sleeping for",dt,"Seconds"
       print "Log Time: ",l_time," Current time: ",datetime.datetime.now()-off_time


# Cleanup
for c in m.handles:
    if c.fp is not None:
        c.fp.close()
        c.fp = None
    c.close()
m.close()

