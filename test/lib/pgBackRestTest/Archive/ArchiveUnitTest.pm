####################################################################################################################################
# ArchiveUnitTest.pm - Tests for ArchiveCommon module
####################################################################################################################################
package pgBackRestTest::Archive::ArchiveUnitTest;
use parent 'pgBackRestTest::Full::FullCommonTest';

####################################################################################################################################
# Perl includes
####################################################################################################################################
use strict;
use warnings FATAL => qw(all);
use Carp qw(confess);

use File::Basename qw(dirname);

use pgBackRest::Archive::ArchiveCommon;
use pgBackRest::Common::Exception;
use pgBackRest::Common::Log;
use pgBackRest::Config::Config;

####################################################################################################################################
# run
####################################################################################################################################
sub run
{
    my $self = shift;
    my $strModule = 'ArchiveCommon';

    #-------------------------------------------------------------------------------------------------------------------------------
    if ($self->begin("${strModule}::walPath()"))
    {
        my $strDbPath = '/db';
        my $strWalFileRelative = 'pg_xlog/000000010000000100000001';
        my $strWalFileAbsolute = "${strDbPath}/${strWalFileRelative}";

        #---------------------------------------------------------------------------------------------------------------------------
        $self->testException(
            sub {walPath($strWalFileRelative, undef, CMD_ARCHIVE_GET)}, ERROR_OPTION_REQUIRED,
            "option '" . OPTION_DB_PATH . "' must be specified when relative xlog paths are used\n" .
            "HINT: Is \%f passed to " . CMD_ARCHIVE_GET . " instead of \%p?\n" .
            "HINT: PostgreSQL may pass relative paths even with \%p depending on the environment.");

        #---------------------------------------------------------------------------------------------------------------------------
        $self->testResult(
            sub {walPath($strWalFileRelative, $strDbPath, CMD_ARCHIVE_PUSH)}, $strWalFileAbsolute, 'relative path is contructed');

        #---------------------------------------------------------------------------------------------------------------------------
        $self->testResult(
            sub {walPath($strWalFileAbsolute, $strDbPath, CMD_ARCHIVE_PUSH)}, $strWalFileAbsolute,
            'path is not relative and db-path is still specified');

        #---------------------------------------------------------------------------------------------------------------------------
        $self->testResult(
            sub {walPath($strWalFileAbsolute, $strDbPath, CMD_ARCHIVE_PUSH)}, $strWalFileAbsolute,
            'path is not relative and db-path is undef');
    }

    #-------------------------------------------------------------------------------------------------------------------------------
    if ($self->begin("${strModule}::walIsSegment()"))
    {
        #---------------------------------------------------------------------------------------------------------------------------
        $self->testResult(sub {walIsSegment('0000000200ABCDEF0000001')}, false, 'invalid segment');

        #---------------------------------------------------------------------------------------------------------------------------
        $self->testResult(sub {walIsSegment('0000000200ABCDEF00000001')}, true, 'valid segment');

        #---------------------------------------------------------------------------------------------------------------------------
        $self->testResult(sub {walIsSegment('000000010000000100000001.partial')}, true, 'valid partial segment');

        #---------------------------------------------------------------------------------------------------------------------------
        $self->testResult(sub {walIsSegment('00000001.history')}, false, 'valid history file');

        #---------------------------------------------------------------------------------------------------------------------------
        $self->testResult(sub {walIsSegment('000000020000000100000001.00000028.backup')}, false, 'valid backup file');
    }
}

1;
