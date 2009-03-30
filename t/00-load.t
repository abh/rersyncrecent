#!perl

use Test::More tests => 1;

BEGIN {
	use_ok( 'File::Rsync::Mirror::Recentfile' );
}

diag( "Testing File::Rsync::Mirror::Recentfile $File::Rsync::Mirror::Recentfile::VERSION, Perl $], $^X" );
