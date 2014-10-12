import sys, re, getopt
import urllib, json

# written by Nick Schmalenberger

def usage():
    print '''
Connects to an HBASE master and monitors active/backup
status and the number of dead region servers.
 check_hbase.py -H <hostname> | [ -b active | backup ]
              [ -w <warn # of dead>[%] ] [ -c <crit # of dead>[%] ]
    -h  This help text.
    -H  The HBASE master hostname to connect to.
    -b  CRITICAL if not the active/backup status given.
        By default either are OK.
    -w  The WARNING threshold for dead region servers.
        If it ends with a % it is taken as a percentage.
        By default, any not negative number is OK.
    -c  The CRITICAL threshold for dead region servers.
        If it ends with a % it is taken as a percentage.
        By default, any not negative number is OK.
'''

# get the options
try:
    opts, args = getopt.getopt(sys.argv[1:], "b:hH:w:c:")
except getopt.GetoptError as err:
    # print help information and exit:
    print str(err)
    usage()
    sys.exit(3)

## initialize some stuff
WARNING=False
WPERCENT=False
CRITICAL=False
CPERCENT=False

for o, a in opts:
    if o == "-h":
        usage()
        sys.exit(3)
    elif o == "-H":
        HOSTNAME=a
    elif o == "-b":
        if a == 'active' or a == 'backup':
            ACTIVEBACKUP=a
        else:
            print "UNKNOWN: invalid argument to -b active|backup : {}".format(a)
            sys.exit(3)
    elif o == "-w":
        if re.match('\d+%',a):
            WARNING=int(re.sub('%','',a))
            WPERCENT=True
        elif re.match('\d+',a):
            WARNING=int(a)
        else:
            print "UNKNOWN: invalid argument to -w <warn # of dead>[%] : {}".format(a)
            sys.exit(3)
    elif o == "-c":
        if re.match('\d+%',a):
            CRITICAL=int(re.sub('%','',a))
            CPERCENT=True
        elif re.match('\d+',a):
            CRITICAL=int(a)
        else:
            print "UNKNOWN: invalid argument to -c <warn # of dead>[%] : {}".format(a)
            sys.exit(3)

## initialize some stuff
try:
    HOSTNAME
except:
    print "Hostname option -H is required."
    usage()
    sys.exit(3)
else:
    HBASE_MASTER_PORT="60010"
    HBASE_MASTERSTATUS_JSONURL="http://{}:{}/master-status?format=json".format(HOSTNAME, HBASE_MASTER_PORT)
    HBASE_JMX_JSONURL="http://{}:{}/jmx".format(HOSTNAME, HBASE_MASTER_PORT)

## here is where we get all the json from the pages
# get the master-status
# it really sucks the IsActiveMaster is nowhere in /jmx on the backup master
# it really sucks the /master-status?format=json output is [] on the active one
try:
    masterstatusjson=urllib.urlopen(HBASE_MASTERSTATUS_JSONURL)
except:
    print "UNKNOWN: Failed to open {}".format(HBASE_MASTERSTATUS_JSONURL)
    sys.exit(3)
# parse the master-status json
try:
    masterstatus=json.load(masterstatusjson)
except:
    print "UNKNOWN: Failed to parse {}".format(HBASE_MASTERSTATUS_JSONURL)
    sys.exit(3)

# try to download the jmx page
try:
    jmxjson=urllib.urlopen(HBASE_JMX_JSONURL)
except:
    print "UNKNOWN: Failed to open {}".format(HBASE_JMX_JSONURL)
    sys.exit(3)
# parse the jmx json
try:
    jmx=json.load(jmxjson)
except:
    print "UNKNOWN: Failed to parse {}".format(HBASE_JMX_JSONURL)
    sys.exit(3)

# we want to find a certain bean in the jmx page
for bean in jmx["beans"]:
    if bean["name"] == "hadoop:service=Master,name=Master":
        jmxmasterinfo=bean


# here is where we find if the host is active or backup
try:
    if re.match('^Another master is the active master, .*; waiting to become the next active master$', masterstatus[0]['status']):
        active=False
        backup=True
except:
    # if this host is in fact active not backup, masterstatus will be []
    pass
try:
    if jmxmasterinfo["IsActiveMaster"] is True:
        active=True
        backup=False
except:
    # if this host is in fact backup not active, jmxmasterinfo will not be set
    # because there was no hadoop:service=Master,name=Master bean
    pass
try:
    active
    backup
except:
    print "UNKNOWN: could not determine active/backup status"
    sys.exit(3)

# here is where we get the region server info (if active)
if active:
    try:
        numactiveregionservers=len(jmxmasterinfo["RegionServers"])
        numdeadregionservers=len(jmxmasterinfo["DeadRegionServers"])
        percentdeadregionservers=(numdeadregionservers/(numactiveregionservers+numdeadregionservers))*100
    except:
        print "UNKNOWN: could not determine number of alive or dead region servers"
        sys.exit(3)

## the output
# the outputs for backup
try:
    ACTIVEBACKUP
except:
    if backup:
        print "OK: {}".format(masterstatus[0]['status'])
        sys.exit(0)
    else:
        pass
else:
    if ACTIVEBACKUP=="active" and not active:
        print "CRITICAL: {}".format(masterstatus[0]['status'])
        sys.exit(2)
    elif ACTIVEBACKUP=="backup" and not backup:
        # if only /jmx would say who the backup master is, we could put that here
        print "CRITICAL: Not the backup master"
        sys.exit(2)
    elif ACTIVEBACKUP=="backup" and backup:
        print "OK: {}".format(masterstatus[0]['status'])
        sys.exit(0)

# the outputs for active       
if not CRITICAL and not WARNING:
    print "OK: {} active region servers. {} dead region servers.".format(numactiveregionservers, numdeadregionservers)
    sys.exit(0)
elif CPERCENT and percentdeadregionservers >= CRITICAL:
    print "CRITICAL: {}% dead region servers".format(percentdeadregionservers)
    sys.exit(2)
elif CRITICAL and numdeadregionservers >= CRITICAL:
    print "CRITICAL: {} dead region servers".format(numdeadregionservers)
    sys.exit(2)
elif WPERCENT and percentdeadregionservers >= WARNING:
    print "WARNING: {}% dead region servers".format(percentdeadregionservers)
    sys.exit(1)
elif WARNING and numdeadregionservers >= WARNING:
    print "WARNING: {} dead region servers".format(numdeadregionservers)
    sys.exit(1)
elif numdeadregionservers >= 0:
    print "OK: {} dead region servers".format(numdeadregionservers)
    sys.exit(0)
else:
    print "UNKNOWN: negative number of dead region servers, or some other problem"
    sys.exit(3)
