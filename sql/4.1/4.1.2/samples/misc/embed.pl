#!/usr/bin/perl

use strict; # Declare using Perl strict syntax

#
# If you are using other Perl's package, declare here
#


open(STDERR, ">&STDOUT");

open(VSQL, "| ".$ENV{"VSQL"});
print VSQL <<ENDOFINPUT;
	\\timing
	-- line comment
	/* block comment */

	\\set v1 `pwd`
	\\echo pwd: :v1
	
	
	\\o /dev/null
	select version();
	select sysdate();
	\\o
	select 'Hello\tworld!';
ENDOFINPUT
close(VSQL);
