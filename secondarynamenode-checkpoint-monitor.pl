#!/usr/bin/perl

use warnings;
use strict;
use diagnostics;

use LWP::Simple;
use DateTimeX::Easy;

my ($host, $warning_periods, $critical_periods, $port, $statusoutput, $checkpoint_dt, $checkpoint_period, $gaptime, $warning_gap, $critical_gap);

# This is a nagios plugin that gets the checkpoint time
# from a Hadoop HDFS secondary namenode, and compares it
# to the current time. The thresholds for the checkpoints
# getting behind are expressed in number of checkpoint
# periods which are also taken from the secondary namenode.

($host, $warning_periods, $critical_periods)=@ARGV;
$port="50090";

$statusoutput=get("http://$host:$port/status.jsp");
die "No response from secondary namenode at http://$host:$port/status.jsp\n" unless defined $statusoutput;

open(my $statusoutput_fh, '<', \$statusoutput) or die $!;

while(<$statusoutput_fh>) {
	if ($_=~/^Last Checkpoint Time +: (.+)$/) {
		$checkpoint_dt=DateTimeX::Easy->new($1);
	}
	elsif ($_=~/Checkpoint Period +: (\d+) seconds$/) {
		$checkpoint_period=$1;
	}
}

$gaptime=time()-$checkpoint_dt->epoch;
$warning_gap=$checkpoint_period*$warning_periods;
$critical_gap=$checkpoint_period*$critical_periods;

if ($gaptime>$critical_gap) {
	print "$host secondary namenode checkpoint $gaptime seconds behind.";
	exit 2;
} elsif ($gaptime>$warning_gap) {
	print "$host secondary namenode checkpoint $gaptime seconds behind.";
	exit 1;
} elsif ($gaptime>0) {
	print "$host secondary namenode checkpoint $gaptime seconds behind.";
	exit 0;
} elsif ($gaptime<0) {
        print "$host secondary namenode checkpoint $gaptime seconds ahead. Check the clocks.";
	exit 1;
}

print "Checkpoint gap could not be calculated, something else is wrong.";
exit 255;
