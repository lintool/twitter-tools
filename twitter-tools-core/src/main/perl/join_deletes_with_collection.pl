#!/usr/bin/perl

# Joins together deletes and collection tweetids to identify the
# deleted statuses.

$USAGE = "$0 [deletes (bz2)] [collection (bz2)]";

$deletes = shift or die $USAGE;
$collection = shift or die $USAGE;

open(DATA, "bzcat $deletes | ");
while ( my $line = <DATA> ) {
    chomp($line);
    $H{$line} = 1;
}
close(DATA);

open(DATA, "bzcat $collection | ");
while ( my $line = <DATA> ) {
    if ($line =~ /^(\d+)/ ) {
	print $line if exists($H{$1});
    }
}
close(DATA);
