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
use Storable qw(dclone);

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
    $oArchiveInfo->create(PG_VERSION_94, WAL_VERSION_94_SYS_ID, true);

    # Set spool path
    $self->{strSpoolPath} = "$self->{strArchivePath}/out";
}

####################################################################################################################################
# clean
####################################################################################################################################
sub clean
{
    my $self = shift;

    executeTest('rm -rf ' . $self->testPath() . '/*');
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
    $self->optionSetTest($oOption, OPTION_REPO_PATH, $self->{strRepoPath});
    $self->optionBoolSetTest($oOption, OPTION_COMPRESS, false);

    ################################################################################################################################
    if ($self->begin("ArchivePushFile::archivePushCheck"))
    {
        $self->clean();
        logDisable(); $self->configLoadExpect(dclone($oOption), CMD_ARCHIVE_PUSH); logEnable();

        #---------------------------------------------------------------------------------------------------------------------------
        my $strWalSegment = '000000010000000100000001';

        $self->testResult(sub {archivePushCheck(
            $self->{oFile}, $strWalSegment, PG_VERSION_94, WAL_VERSION_94_SYS_ID, "$self->{strWalPath}/${strWalSegment}")},
            '(9.4-1, [undef])', "${strWalSegment} WAL not found");

        #---------------------------------------------------------------------------------------------------------------------------
        my $strWalMajorPath = "$self->{strArchivePath}/9.4-1/" . substr($strWalSegment, 0, 16);
        my $strWalSegmentHash = "${strWalSegment}-1e34fa1c833090d94b9bb14f2a8d3153dca6ea27";

        $self->walGenerate(
            $self->{oFile}, $self->{strWalPath}, WAL_VERSION_94, 1, $strWalSegment);

        filePathCreate($strWalMajorPath, undef, false, true);
        fileStringWrite("${strWalMajorPath}/${strWalSegmentHash}");

        $self->testResult(sub {archivePushCheck(
            $self->{oFile}, $strWalSegment, PG_VERSION_94, WAL_VERSION_94_SYS_ID, "$self->{strWalPath}/${strWalSegment}")},
            '(9.4-1, 1e34fa1c833090d94b9bb14f2a8d3153dca6ea27)', "${strWalSegment} WAL found");

        fileRemove("${strWalMajorPath}/${strWalSegmentHash}");

        #---------------------------------------------------------------------------------------------------------------------------
        $strWalSegmentHash = "${strWalSegment}-10be15a0ab8e1653dfab18c83180e74f1507cab1";

        fileStringWrite("${strWalMajorPath}/${strWalSegmentHash}");

        $self->testException(sub {archivePushCheck(
            $self->{oFile}, $strWalSegment, PG_VERSION_94, WAL_VERSION_94_SYS_ID, "$self->{strWalPath}/${strWalSegment}")},
            ERROR_ARCHIVE_DUPLICATE, "WAL segment ${strWalSegment} already exists in the archive");

        #---------------------------------------------------------------------------------------------------------------------------
        $strWalSegment = "${strWalSegment}.partial";
        $strWalSegmentHash = "${strWalSegment}-1e34fa1c833090d94b9bb14f2a8d3153dca6ea27";

        $self->walGenerate(
            $self->{oFile}, $self->{strWalPath}, WAL_VERSION_94, 1, $strWalSegment);

        fileStringWrite("${strWalMajorPath}/${strWalSegmentHash}");

        $self->testResult(sub {archivePushCheck(
            $self->{oFile}, $strWalSegment, PG_VERSION_94, WAL_VERSION_94_SYS_ID, "$self->{strWalPath}/${strWalSegment}")},
            '(9.4-1, 1e34fa1c833090d94b9bb14f2a8d3153dca6ea27)', "${strWalSegment} WAL found");

        fileRemove("${strWalMajorPath}/${strWalSegmentHash}");

        #---------------------------------------------------------------------------------------------------------------------------
        $strWalSegmentHash = "${strWalSegment}-10be15a0ab8e1653dfab18c83180e74f1507cab1";

        fileStringWrite("${strWalMajorPath}/${strWalSegmentHash}");

        $self->testException(sub {archivePushCheck(
            $self->{oFile}, $strWalSegment, PG_VERSION_94, WAL_VERSION_94_SYS_ID, "$self->{strWalPath}/${strWalSegment}")},
            ERROR_ARCHIVE_DUPLICATE, "WAL segment ${strWalSegment} already exists in the archive");

        #---------------------------------------------------------------------------------------------------------------------------
        $self->testException(sub {archivePushCheck(
            $self->{oFile}, $strWalSegment, PG_VERSION_94, WAL_VERSION_94_SYS_ID)},
            ERROR_ASSERT, "strFile is required in File->hash");

        #---------------------------------------------------------------------------------------------------------------------------
        my $strHistoryFile = "00000001.history";

        fileStringWrite("$self->{strArchivePath}/9.4-1/${strHistoryFile}");

        $self->testResult(sub {archivePushCheck(
            $self->{oFile}, $strHistoryFile, PG_VERSION_94, WAL_VERSION_94_SYS_ID, "$self->{strWalPath}/${strHistoryFile}")},
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

        my $oPushAsync = new pgBackRest::Archive::ArchivePushAsync($self->{strWalPath}, $self->{strSpoolPath});
        logDisable(); $self->configLoadExpect(dclone($oOption), CMD_ARCHIVE_PUSH); logEnable();
        $oPushAsync->initServer();

        my $iWalTimeline = 1;
        my $iWalMajor = 1;
        my $iWalMinor = 1;

        #---------------------------------------------------------------------------------------------------------------------------
        fileStringWrite(
            "$self->{strWalStatusPath}/" . $self->walSegment($iWalTimeline, $iWalMajor, $iWalMinor++) . '.done');

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

        fileStringWrite("$self->{strWalStatusPath}/00000002.history.ready");

        $self->testResult(
            sub {$oPushAsync->readyList()}, '(00000002.history)',
            'history .ready file');

        #---------------------------------------------------------------------------------------------------------------------------
        fileStringWrite(
            "$self->{strWalStatusPath}/" . $self->walSegment($iWalTimeline, $iWalMajor, $iWalMinor++) . '.00000028.backup.ready');

        $self->testResult(
            sub {$oPushAsync->readyList()}, '(000000020000000100000001.00000028.backup)',
            'backup .ready file');
    }

    ################################################################################################################################
    if ($self->begin("ArchivePushAsync->process()"))
    {
        $self->clean();

        my $oPushAsync = new pgBackRest::Archive::ArchivePushAsync(
            $self->{strWalPath}, $self->{strSpoolPath}, $self->backrestExe());
        logDisable(); $self->configLoadExpect(dclone($oOption), CMD_ARCHIVE_PUSH); logEnable();
        $oPushAsync->initServer();

        my $iWalTimeline = 1;
        my $iWalMajor = 1;
        my $iWalMinor = 1;

        #---------------------------------------------------------------------------------------------------------------------------
        # Generate a normal segment
        my $strSegment = $self->walSegment($iWalTimeline, $iWalMajor, $iWalMinor++);
        $self->walGenerate($self->{oFile}, $self->{strWalPath}, WAL_VERSION_94, 1, $strSegment);

        # Generate an error (.ready file withough a corresponding WAL file)
        my $strSegmentError = $self->walSegment($iWalTimeline, $iWalMajor, $iWalMinor++);
        fileStringWrite("$self->{strWalStatusPath}/$strSegmentError.ready");

        # Process and check results
        $self->testResult(sub {$oPushAsync->processQueue()}, '(2, 1, 1)', "process ${strSegment}, ${strSegmentError}");

        $self->testResult(
            sub {fileList($self->{strSpoolPath})}, "(${strSegment}.ok, ${strSegmentError}.error)",
            "${strSegment} pushed, ${strSegmentError} errored");

        $self->testResult(
            sub {fileStringRead("$self->{strSpoolPath}/$strSegmentError.error")},
            ERROR_FILE_OPEN . "\nraised on local-1 host: unable to open $self->{strWalPath}/${strSegmentError}",
            "test ${strSegmentError}.error contents");

        #---------------------------------------------------------------------------------------------------------------------------
        # Remove pushed WAL file
        $self->walRemove($self->{strWalPath}, $strSegment);

        # Fix errored WAL file by providing a valid segment
        $self->walGenerate($self->{oFile}, $self->{strWalPath}, WAL_VERSION_94, 1, $strSegmentError);

        # Process and check results
        $self->testResult(sub {$oPushAsync->processQueue()}, '(1, 1, 0)', "process ${strSegment}, ${strSegmentError}");

        $self->testResult(sub {fileList($self->{strSpoolPath})}, "${strSegmentError}.ok", "${strSegmentError} pushed");

        #---------------------------------------------------------------------------------------------------------------------------
        # Remove previously errored WAL file
        $self->walRemove($self->{strWalPath}, $strSegmentError);

        # Process and check results
        $self->testResult(sub {$oPushAsync->processQueue()}, '(0, 0, 0)', "remove ${strSegmentError}.ready");

        $self->testResult(sub {fileList($self->{strSpoolPath})}, "[undef]", "${strSegmentError} removed");

        #---------------------------------------------------------------------------------------------------------------------------
        # Create history file
        my $strHistoryFile = "00000001.history";

        fileStringWrite("$self->{strWalPath}/${strHistoryFile}");
        fileStringWrite("$self->{strWalStatusPath}/$strHistoryFile.ready");

        # Create backup file
        my $strBackupFile = "${strSegment}.00000028.backup";

        fileStringWrite("$self->{strWalPath}/${strBackupFile}");
        fileStringWrite("$self->{strWalStatusPath}/$strBackupFile.ready");

        # Process and check results
        $self->testResult(sub {$oPushAsync->processQueue();}, '(2, 2, 0)', "end processing ${strHistoryFile}, ${strBackupFile}");

        $self->testResult(
            sub {fileList($self->{strSpoolPath})}, "(${strHistoryFile}.ok, ${strBackupFile}.ok)",
            "${strHistoryFile}, ${strBackupFile} pushed");

        # Remove history and backup files
        fileRemove("$self->{strWalStatusPath}/$strHistoryFile.ready");
        fileRemove("$self->{strWalStatusPath}/$strBackupFile.ready");

        #---------------------------------------------------------------------------------------------------------------------------
        # Enable compression
        $self->optionBoolSetTest($oOption, OPTION_COMPRESS, true);
        logDisable(); $self->configLoadExpect(dclone($oOption), CMD_ARCHIVE_PUSH); logEnable();

        # Generate a normal segment
        $strSegment = $self->walSegment($iWalTimeline, $iWalMajor, $iWalMinor++);
        $self->walGenerate($self->{oFile}, $self->{strWalPath}, WAL_VERSION_94, 1, $strSegment);

        # Process and check results
        $self->testResult(sub {$oPushAsync->processQueue();}, '(1, 1, 0)', "processing ${strSegment}.gz");

        # Remove the WAL and process so the .ok file is removed
        $self->walRemove($self->{strWalPath}, $strSegment);

        $self->testResult(sub {$oPushAsync->processQueue();}, '(0, 0, 0)', "remove ${strSegment}.ready");

        $self->testResult(sub {fileList($self->{strSpoolPath})}, "[undef]", "${strSegment}.ok removed");

        # Generate the same WAL again
        $self->walGenerate($self->{oFile}, $self->{strWalPath}, WAL_VERSION_94, 1, $strSegment);

        # Process and check results
        $self->testResult(sub {$oPushAsync->processQueue();}, '(1, 1, 0)', "processed duplicate ${strSegment}.gz");

        $self->testResult(sub {fileList($self->{strSpoolPath})}, "000000010000000100000003.ok", "${strSegment} pushed");
    }

    ################################################################################################################################
    if ($self->begin("ArchivePush->walStatus()"))
    {
        $self->clean();
        my $oPush = new pgBackRest::Archive::ArchivePush();

        my $iWalTimeline = 1;
        my $iWalMajor = 1;
        my $iWalMinor = 1;

        #---------------------------------------------------------------------------------------------------------------------------
        my $strSegment = $self->walSegment($iWalTimeline, $iWalMajor, $iWalMinor++);

        $self->testResult(sub {$oPush->walStatus($self->{strSpoolPath}, $strSegment);}, '0',  "${strSegment} WAL no status");

        #---------------------------------------------------------------------------------------------------------------------------
        # Generate a normal ok
        filePathCreate($self->{strSpoolPath}, undef, undef, true);
        fileStringWrite("$self->{strSpoolPath}/${strSegment}.ok");

        # Check status
        $self->testResult(sub {$oPush->walStatus($self->{strSpoolPath}, $strSegment);}, '1',  "${strSegment} WAL ok");

        #---------------------------------------------------------------------------------------------------------------------------
        # Generate a bogus warning ok (if content is present there must be two lines)
        $strSegment = $self->walSegment($iWalTimeline, $iWalMajor, $iWalMinor++);
        fileStringWrite("$self->{strSpoolPath}/${strSegment}.ok", "Test Warning");

        # Check status
        $self->testException(sub {$oPush->walStatus($self->{strSpoolPath}, $strSegment);}, ERROR_ASSERT,
            "${strSegment}.ok content must have at least two lines:\nTest Warning");

        #---------------------------------------------------------------------------------------------------------------------------
        # Generate a valid warning ok
        fileStringWrite("$self->{strSpoolPath}/${strSegment}.ok", "0\nTest Warning");

        # Check status
        $self->testResult(sub {$oPush->walStatus($self->{strSpoolPath}, $strSegment);}, '1',  "${strSegment} WAL warning ok");

        #---------------------------------------------------------------------------------------------------------------------------
        # Generate an invalid error
        fileStringWrite("$self->{strSpoolPath}/${strSegment}.error");

        # Check status (will error because there are now two status files)
        $self->testException(sub {$oPush->walStatus($self->{strSpoolPath}, $strSegment);}, ERROR_ASSERT,
            "multiple status files found in /home/vagrant/test/test-0/repo/archive/db/out for ${strSegment}:" .
            " ${strSegment}.error, ${strSegment}.ok");

        #---------------------------------------------------------------------------------------------------------------------------
        # Remove the ok file
        fileRemove("$self->{strSpoolPath}/${strSegment}.ok");

        # Check status
        $self->testException(
            sub {$oPush->walStatus($self->{strSpoolPath}, $strSegment);}, ERROR_ASSERT, "${strSegment}.error has no content");

        #---------------------------------------------------------------------------------------------------------------------------
        # Generate a valid error
        fileStringWrite(
            "$self->{strSpoolPath}/${strSegment}.error",
            ERROR_ARCHIVE_DUPLICATE . "\nWAL segment ${strSegment} already exists in the archive");

        # Check status
        $self->testException(sub {$oPush->walStatus($self->{strSpoolPath}, $strSegment);}, ERROR_ARCHIVE_DUPLICATE,
            "WAL segment ${strSegment} already exists in the archive");

        #---------------------------------------------------------------------------------------------------------------------------
        # Change the error file to an ok file
        fileMove("$self->{strSpoolPath}/${strSegment}.error", "$self->{strSpoolPath}/${strSegment}.ok");

        # Check status
        $self->testResult(
            sub {$oPush->walStatus($self->{strSpoolPath}, $strSegment);}, '1',
            "${strSegment} WAL warning ok (converted from .error)");

        #---------------------------------------------------------------------------------------------------------------------------
        # Generate a normal ok
        $strSegment = $self->walSegment($iWalTimeline, $iWalMajor, $iWalMinor++);
        fileStringWrite("$self->{strSpoolPath}/${strSegment}.ok");

        #---------------------------------------------------------------------------------------------------------------------------
        $strSegment = $self->walSegment($iWalTimeline, $iWalMajor, $iWalMinor++);

        # Check status
        $self->testResult(sub {$oPush->walStatus($self->{strSpoolPath}, $strSegment);}, '0',  "${strSegment} WAL no status");
    }
}

1;
