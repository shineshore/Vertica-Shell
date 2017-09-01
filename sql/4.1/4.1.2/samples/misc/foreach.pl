#!/usr/bin/perl

use strict; # Declare using Perl strict syntax

my $VSQLCMD=$ENV{"VSQL"};
$VSQLCMD =~ s/-a//g ;
$VSQLCMD =~ s/-e//g ;

open (VSQL, $VSQLCMD . " -Atq -c 'select table_name from system_tables limit 10' |");
while (<VSQL>) {
	my $tableName=$_;
	print $tableName;
}
close(VSQL);

