####################################################################################################################################
# ArchivePushUnitTest.pm - Unit tests for ArchivePush and ArchivePush Async
####################################################################################################################################
package pgBackRestTest::Archive::ArchivePushUnitTest;
use parent 'pgBackRestTest::Full::FullCommonTest';

####################################################################################################################################
# Perl includes
####################################################################################################################################
use strict;
use warnings FATAL => qw(all);
use Carp qw(confess);

use File::Basename qw(dirname);

# use pgBackRest::Archive::ArchiveInfo;
use pgBackRest::Archive::ArchivePushAsync;
# use pgBackRest::DbVersion;
use pgBackRest::Common::Exception;
# use pgBackRest::Common::Ini;
use pgBackRest::Common::Log;
# use pgBackRest::Common::Wait;
use pgBackRest::Config::Config;
# use pgBackRest::File;
use pgBackRest::FileCommon;
# use pgBackRest::Manifest;

use pgBackRestTest::Common::ExecuteTest;
use pgBackRestTest::Common::Host::HostBackupTest;
# use pgBackRestTest::Common::RunTest;
use pgBackRestTest::Full::FullCommonTest;

####################################################################################################################################
# archiveCheck
#
# Check that a WAL segment is present in the repository.
####################################################################################################################################
# sub archiveCheck
# {
#     my $self = shift;
#     my $oFile = shift;
#     my $strArchiveFile = shift;
#     my $strArchiveChecksum = shift;
#     my $bCompress = shift;
#
#     # Build the archive name to check for at the destination
#     my $strArchiveCheck = WAL_VERSION_94 . "-1/${strArchiveFile}-${strArchiveChecksum}";
#
#     if ($bCompress)
#     {
#         $strArchiveCheck .= '.gz';
#     }
#
#     my $oWait = waitInit(5);
#     my $bFound = false;
#
#     do
#     {
#         $bFound = $oFile->exists(PATH_BACKUP_ARCHIVE, $strArchiveCheck);
#     }
#     while (!$bFound && waitMore($oWait));
#
#     if (!$bFound)
#     {
#         confess 'unable to find ' . $strArchiveCheck;
#     }
# }

####################################################################################################################################
# init
####################################################################################################################################
sub init
{
    my $self = shift;

    $self->{strDbPath} = $self->testPath() . '/db';
    $self->{strWalPath} = "$self->{strDbPath}/pg_xlog";
    $self->{strWalStatusPath} = "$self->{strWalPath}/archive_status";
    $self->{strRepoPath} = $self->testPath() . '/repo';

    # Create the local file object
    $self->{oFile} =
        new pgBackRest::File
        (
            $self->stanza(),
            $self->{strRepoPath},
            new pgBackRest::Protocol::Common
            (
                OPTION_DEFAULT_BUFFER_SIZE,                 # Buffer size
                OPTION_DEFAULT_COMPRESS_LEVEL,              # Compress level
                OPTION_DEFAULT_COMPRESS_LEVEL_NETWORK,      # Compress network level
                HOST_PROTOCOL_TIMEOUT                       # Protocol timeout
            )
        );

    filePathCreate($self->{strWalStatusPath}, undef, true, true);
}

####################################################################################################################################
# clean
####################################################################################################################################
sub clean
{
    my $self = shift;

    executeTest("rm -rf $self->{strWalPath}");
    $self->init();
}

####################################################################################################################################
# run
####################################################################################################################################
sub run
{
    my $self = shift;

    my $oOption = {};

    $self->optionSetTest($oOption, OPTION_STANZA, $self->stanza());
    $self->optionSetTest($oOption, OPTION_DB_PATH, $self->{strDbPath});

    my $oPushAsync = new pgBackRest::Archive::ArchivePushAsync($self->{strWalPath}, $self->testPath() . '/archive-push.socket');

    #-------------------------------------------------------------------------------------------------------------------------------
    if ($self->begin("ArchivePushAsync->readyList"))
    {
        $self->clean();

        my $iWalTimeline = 1;
        my $iWalMajor = 1;
        my $iWalMinor = 1;

        #---------------------------------------------------------------------------------------------------------------------------
        fileStringWrite(
            "$self->{strWalStatusPath}/" . $self->walSegment($iWalTimeline, $iWalMajor, $iWalMinor++) . '.done', 'TEST');

        $self->testResult(
            sub {$oPushAsync->readyList()}, '()',
            'ignore files without .ready extenstion');

        #---------------------------------------------------------------------------------------------------------------------------
        $self->walGenerate(
            $self->{oFile}, $self->{strWalPath}, WAL_VERSION_94, 1, $self->walSegment($iWalTimeline, $iWalMajor, $iWalMinor++));
        $self->walGenerate(
            $self->{oFile}, $self->{strWalPath}, WAL_VERSION_94, 1, $self->walSegment($iWalTimeline, $iWalMajor, $iWalMinor++));

        $self->testResult(
            sub {$oPushAsync->readyList()}, '(000000010000000100000002, 000000010000000100000003)',
            '.ready files are found');

        #---------------------------------------------------------------------------------------------------------------------------
        $self->walGenerate(
            $self->{oFile}, $self->{strWalPath}, WAL_VERSION_94, 1, $self->walSegment($iWalTimeline, $iWalMajor, $iWalMinor++));

        $self->testResult(
            sub {$oPushAsync->readyList()}, '(000000010000000100000004)',
            'new .ready files are found and duplicates ignored');

        #---------------------------------------------------------------------------------------------------------------------------
        $self->testResult(
            sub {$oPushAsync->readyList()}, '()',
            'no new .ready files returns empty list');

        #---------------------------------------------------------------------------------------------------------------------------
        $iWalTimeline++;
        $iWalMinor = 1;

        fileStringWrite("$self->{strWalStatusPath}/00000002.history.ready", 'TEST');

        $self->testResult(
            sub {$oPushAsync->readyList()}, '(00000002.history)',
            'history .ready file');

        #---------------------------------------------------------------------------------------------------------------------------
        fileStringWrite(
            "$self->{strWalStatusPath}/" . $self->walSegment($iWalTimeline, $iWalMajor, $iWalMinor++) . '.00000028.backup.ready',
            'TEST');

        $self->testResult(
            sub {$oPushAsync->readyList()}, '(000000020000000100000001.00000028.backup)',
            'backup .ready file');
    }

    #-------------------------------------------------------------------------------------------------------------------------------
    if ($self->begin("ArchivePushAsync"))
    {
        $self->clean();

        my $iWalTimeline = 1;
        my $iWalMajor = 1;
        my $iWalMinor = 1;

        logDisable(); $self->configLoadExpect($oOption, CMD_ARCHIVE_PUSH); logEnable();

        $self->walGenerate(
            $self->{oFile}, $self->{strWalPath}, WAL_VERSION_94, 1, $self->walSegment($iWalTimeline, $iWalMajor, $iWalMinor++));

        $oPushAsync->initServer();
        $oPushAsync->processQueue();
    }
}

1;
