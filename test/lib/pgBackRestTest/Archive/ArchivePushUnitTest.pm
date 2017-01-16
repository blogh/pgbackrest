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

use pgBackRest::Archive::ArchivePushAsync;
use pgBackRest::Archive::ArchivePushFile;
use pgBackRest::Common::Exception;
use pgBackRest::Common::Log;
use pgBackRest::Config::Config;
use pgBackRest::DbVersion;
use pgBackRest::FileCommon;

use pgBackRestTest::Common::ExecuteTest;
use pgBackRestTest::Common::Host::HostBackupTest;
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

    # Create WAL path
    filePathCreate($self->{strWalStatusPath}, undef, true, true);

    # Create archive info
    $self->{strArchivePath} = "$self->{strRepoPath}/archive/" . $self->stanza();
    filePathCreate($self->{strArchivePath}, undef, true, true);

    my $oArchiveInfo = new pgBackRest::Archive::ArchiveInfo($self->{strArchivePath}, false);
    $oArchiveInfo->create(PG_VERSION_94, WAL_VERSION_94_SYS_ID, true)
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
    $self->optionSetTest($oOption, OPTION_DB_TIMEOUT, 5);
    $self->optionSetTest($oOption, OPTION_PROTOCOL_TIMEOUT, 6);

    ################################################################################################################################
    if ($self->begin("ArchivePushFile::archivePushCheck"))
    {
        $self->clean();

        #---------------------------------------------------------------------------------------------------------------------------
        my $strWalSegment = '000000010000000100000001';

        $self->testResult(sub {archivePushCheck(
            $self->{oFile}, $strWalSegment, "$self->{strWalPath}/${strWalSegment}", PG_VERSION_94, WAL_VERSION_94_SYS_ID)},
            '(9.4-1, [undef])', "${strWalSegment} WAL not found");

        #---------------------------------------------------------------------------------------------------------------------------
        my $strWalMajorPath = "$self->{strArchivePath}/9.4-1/" . substr($strWalSegment, 0, 16);
        my $strWalSegmentHash = "${strWalSegment}-1e34fa1c833090d94b9bb14f2a8d3153dca6ea27";

        $self->walGenerate(
            $self->{oFile}, $self->{strWalPath}, WAL_VERSION_94, 1, $strWalSegment);

        filePathCreate($strWalMajorPath, undef, false, true);
        fileStringWrite("${strWalMajorPath}/${strWalSegmentHash}", "TEST");

        $self->testResult(sub {archivePushCheck(
            $self->{oFile}, $strWalSegment, "$self->{strWalPath}/${strWalSegment}", PG_VERSION_94, WAL_VERSION_94_SYS_ID);},
            '(9.4-1, 1e34fa1c833090d94b9bb14f2a8d3153dca6ea27)', "${strWalSegment} WAL found");

        fileRemove("${strWalMajorPath}/${strWalSegmentHash}");

        #---------------------------------------------------------------------------------------------------------------------------
        $strWalSegmentHash = "${strWalSegment}-10be15a0ab8e1653dfab18c83180e74f1507cab1";

        fileStringWrite("${strWalMajorPath}/${strWalSegmentHash}", "TEST");

        $self->testException(sub {archivePushCheck(
            $self->{oFile}, $strWalSegment, "$self->{strWalPath}/${strWalSegment}", PG_VERSION_94, WAL_VERSION_94_SYS_ID)},
            ERROR_ARCHIVE_DUPLICATE, "WAL segment ${strWalSegment} already exists in the archive");

        #---------------------------------------------------------------------------------------------------------------------------
        my $strHistoryFile = "00000001.history";

        fileStringWrite("$self->{strArchivePath}/9.4-1/${strHistoryFile}", "TEST");

        $self->testResult(sub {archivePushCheck(
            $self->{oFile}, $strHistoryFile, "$self->{strWalPath}/${strHistoryFile}", PG_VERSION_94, WAL_VERSION_94_SYS_ID);},
            '(9.4-1, [undef])', "history file ${strHistoryFile} found");
    }

    ################################################################################################################################
    if ($self->begin("ArchivePushFile::archivePushFile"))
    {
        $self->clean();
    }

    ################################################################################################################################
    if ($self->begin("ArchivePushAsync->readyList()"))
    {
        $self->clean();

        my $oPushAsync = new pgBackRest::Archive::ArchivePushAsync(
            $self->{strWalPath}, $self->testPath() . '/archive-push.socket');

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

    ################################################################################################################################
    if ($self->begin("ArchivePushAsync->process()"))
    {
        $self->clean();

        my $oPushAsync = new pgBackRest::Archive::ArchivePushAsync(
            $self->{strWalPath}, $self->testPath() . '/archive-push.socket', $self->backrestExe());

        my $iWalTimeline = 1;
        my $iWalMajor = 1;
        my $iWalMinor = 1;

        logDisable(); $self->configLoadExpect($oOption, CMD_ARCHIVE_PUSH); logEnable();

        my $strSegment = $self->walSegment($iWalTimeline, $iWalMajor, $iWalMinor++);
        $self->walGenerate($self->{oFile}, $self->{strWalPath}, WAL_VERSION_94, 1, $strSegment);

        $oPushAsync->initServer();

        #---------------------------------------------------------------------------------------------------------------------------
        $self->testResult(sub {$oPushAsync->processQueue()}, '(1, 1)', "begin processing ${strSegment}");

        $self->testResult($oPushAsync->{hWalState}, '{000000010000000100000001 => 0}', "${strSegment} not pushed");

        #---------------------------------------------------------------------------------------------------------------------------
        $self->testResult(sub {$oPushAsync->processQueue();}, '(0, 0)', "end processing ${strSegment}");

        $self->testResult($oPushAsync->{hWalState}, '{000000010000000100000001 => 1}', "${strSegment} pushed");

        #---------------------------------------------------------------------------------------------------------------------------
        $self->walRemove($self->{strWalPath}, $strSegment);

        $self->testResult(sub {$oPushAsync->processQueue()}, '(0, 0)', "${strSegment}.ready removed");

        $self->testResult($oPushAsync->{hWalState}, '{}', "${strSegment} pushed");
    }
}

1;
