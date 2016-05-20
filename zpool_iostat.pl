#!/usr/perl5/bin/perl -w

# Pushes zpool metrics to carbon: read ops, write ops, read mb/s , write mb/s
# Uses zpool iostat
# output of bandwitdh in mb/s
#
# Graphite path: iostat.{hostname}.{zfs}.metric
#
# Using 10 iterations of  iostat with 5 second delay (50 seconds total), for which an average is calculated
# (so 9 iterations as the first one is an average)
# This can eg be run every minute
#

use strict;
#use Data::Dumper;
use IO::Socket;

######################################
my $debug=0;
my $iostat_iterations=10;
my $carbon_server='graphite';
my $carbon_port='2003';
######################################

my @iostat=`zpool iostat 5 $iostat_iterations`;
my $time=time();
my $hostname=`hostname`;

chomp $hostname;

my %stats;

foreach my $line (@iostat) {
	chomp $line;
	if ( $line =~ /--/ ) { next; }
	my ($zfs, $rops, $wops, $rmb, $wmb);
	if ( $line =~ /(\S+)\s+\S+\s+\S+\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)/ ) {
		$zfs=$1;
		$rops=$2;
		$wops=$3;
		$rmb=$4;
		$wmb=$5;

		# note to self: please implement this in a properly fashion ...
		# switch kb to mb (add gb later on too)
		# should be put in a seperate fucntion
		# also needs some validation
		if ( $rmb =~ /(\S+)M/ ) {
			 $rmb=$1;
		} elsif ( $rmb =~ /(\S+)K/ ) { 
			$rmb=$1/1024;
		}
	
		if ( $wmb =~ /(\S+)M/ ) {
			 $wmb=$1;
		} elsif ( $wmb =~ /(\S+)K/ ) { 
			$wmb=$1/1024;
		}
		                if ( $rops =~ /(\S+)M/ ) {
                         $rops=$1*1000*1000;
                } elsif ( $rops =~ /(\S+)K/ ) {
                        $rops=$1*1000;
                }
        
                if ( $wops =~ /(\S+)M/ ) {
                         $wops=$1*1000*1000;
                } elsif ( $wops =~ /(\S+)K/ ) {
                        $wops=$1*1000;
                }

		print "$line\n" if $debug;
		print "zfs: $zfs  -- rops: $rops -- wops: $wops -- rmb: $rmb -- wmb: $wmb\n\n" if $debug;

		# If this is the first occurence, initialize the counters as the first iostat output line is an average
		if ( !$stats{$zfs} ) {
			$stats{$zfs}{'rops'}=0;
			$stats{$zfs}{'wops'}=0;
			$stats{$zfs}{'rmb'}=0;
			$stats{$zfs}{'wmb'}=0;
			#next;
		} else {
			# update the stats
			$stats{$zfs}{'rops'}+=$rops;
			$stats{$zfs}{'wops'}+=$wops;
			$stats{$zfs}{'rmb'}+=$rmb;
			$stats{$zfs}{'wmb'}+=$wmb;
		}
	
	}
}

## Prep the socket
# code from benr http://cuddletech.com/blog/?category_name=solaris
my $sock = IO::Socket::INET->new(
    Proto    => 'tcp',
    PeerPort => $carbon_port,
    PeerAddr => $carbon_server,
) or die "Could not create socket: $!\n";

# print Dumper %stats;
# loop through the zfs and calculate an average for each metric
# divide by ($iostat_iterations-1) as we only have that many meaningfull metrics
for my $zfs (keys %stats) {
	my $mrops=$stats{$zfs}{'rops'}/($iostat_iterations-1);
	my $mwops=$stats{$zfs}{'wops'}/($iostat_iterations-1);
	my $mrmb=$stats{$zfs}{'rmb'}/($iostat_iterations-1);
	my $mwmb=$stats{$zfs}{'wmb'}/($iostat_iterations-1);
	
	print "$zfs $mrops $mwops $mrmb $mwmb $time\n" if $debug;
	$sock->send("iostat\.$hostname\.$zfs.rops $mrops $time\n") or die "Send error: $!\n";
	$sock->send("iostat\.$hostname\.$zfs.wops $mwops $time\n") or die "Send error: $!\n";
	$sock->send("iostat\.$hostname\.$zfs.rmb $mrmb $time\n") or die "Send error: $!\n";
	$sock->send("iostat\.$hostname\.$zfs.wmb $mwmb $time\n") or die "Send error: $!\n";

}

