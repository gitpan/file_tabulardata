#!/usr/local/bin/perl

use strict;
use warnings;

use ExtUtils::MakeMaker;

WriteMakefile(
    NAME            => 'File::TabularData',
    VERSION_FROM    => 'lib/File/TabularData.pm'
);

exit;
