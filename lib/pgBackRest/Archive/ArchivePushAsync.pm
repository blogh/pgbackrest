####################################################################################################################################
# ARCHIVE PUSH ASYNC MODULE
####################################################################################################################################
package pgBackRest::Archive::ArchivePushAsync;
use parent 'pgBackRest::Archive::ArchivePush';

use strict;
use warnings FATAL => qw(all);
use Carp qw(confess);
use English '-no_match_vars';

use Exporter qw(import);
    our @EXPORT = qw();
use Fcntl qw(SEEK_CUR O_RDONLY O_WRONLY O_CREAT);
use File::Basename qw(dirname basename);
use IO::Socket::UNIX;
use POSIX qw(setsid);
use Scalar::Util qw(blessed);

use pgBackRest::Common::Exception;
use pgBackRest::Common::Lock;
use pgBackRest::Common::Log;
use pgBackRest::Archive::ArchiveCommon;
use pgBackRest::Archive::ArchiveInfo;
use pgBackRest::Common::String;
use pgBackRest::Common::Wait;
use pgBackRest::Config::Config;
use pgBackRest::Db;
use pgBackRest::DbVersion;
use pgBackRest::File;
use pgBackRest::FileCommon;
use pgBackRest::Protocol::ArchivePushMaster;
use pgBackRest::Protocol::ArchivePushMinion;
use pgBackRest::Protocol::Common;
use pgBackRest::Protocol::Protocol;

####################################################################################################################################
# constructor
####################################################################################################################################
sub new
{
    my $class = shift;          # Class name

    # Init object
    my $self = $class->SUPER::new();
    bless $self, $class;

    # Assign function parameters, defaults, and log debug info
    (
        my $strOperation,
        $self->{strWalPath},
        $self->{strWalFileBegin},
    ) =
        logDebugParam
        (
            __PACKAGE__ . '->new', \@_,
            {name => 'strWalPath'},
            {name => 'strWalFileBegin'}
        );

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'self', value => $self}
    );
}

####################################################################################################################################
# process
####################################################################################################################################
sub process
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my ($strOperation) = logDebugParam(__PACKAGE__ . '->process');

    # If logging locally then create the stop archiving file name
    # my $strStopFile;
    #
    # if ($bArchiveAsync)
    # {
    #     $strStopFile = optionGet(OPTION_SPOOL_PATH) . '/stop/' . optionGet(OPTION_STANZA) . "-archive.stop";
    # }

    # If the stop file exists then discard the archive log
    # if ($bArchiveAsync)
    # {
    #     if (-e $strStopFile)
    #     {
    #         &log(ERROR, "discarding " . basename($ARGV[1]) .
    #                     " due to the archive store max size exceeded" .
    #                     " - remove the archive stop file (${strStopFile}) to resume archiving" .
    #                     " and be sure to take a new backup as soon as possible");
    #         return 0;
    #     }
    # }

    # Create a lock file to make sure async archive-push does not run more than once
    my $bClient = true;
    my $strSocketFile = optionGet(OPTION_LOCK_PATH) . '/archive-push.socket';

    if (lockAcquire(commandGet(), false))
    {
        # Remove the old socket file
        fileRemove($strSocketFile, true);
        $bClient = fork() == 0 ? false : true;
    }
    else
    {
        logDebugMisc($strOperation, 'async archive-push process is already running');
    }

    if ($bClient)
    {
        my $iWaitSeconds = 10;
        my $oWait = waitInit($iWaitSeconds);

        # Wait for the socket file to appear
        my $bExists = false;

        do
        {
            $bExists = fileExists($strSocketFile);
        }
        while (!$bExists && waitMore($oWait));

        if (!$bExists)
        {
            confess &log(ERROR, "unable to find socket after ${iWaitSeconds} second(s)", ERROR_ARCHIVE_TIMEOUT);
        }

        my $oSocket = logErrorResult(
            IO::Socket::UNIX->new(Type => SOCK_STREAM, Peer => $strSocketFile), ERROR_ARCHIVE_TIMEOUT,
            'unable to connect to ' . CMD_ARCHIVE_PUSH . " async process socket: ${strSocketFile}");

        $self->processClient(new pgBackRest::Protocol::ArchivePushMaster($oSocket));

    }
    else
    {
        chdir '/'
            or confess "chdir() failed: $!";

        # close stdin/stdout/stderr
        open STDIN, '<', '/dev/null'
            or confess "Couldn't close stdin: $!";
        open STDOUT, '>', '/dev/null'
            or confess "Couldn't close stdout: $!";
        open STDERR, '>', '/dev/null'
            or confess "Couldn't close stderr: $!";

        # create new session group
        setsid() or confess("setsid() failed: $!");

        # Open the log file
        logFileSet(optionGet(OPTION_LOG_PATH) . '/' . optionGet(OPTION_STANZA) . '-archive-async');

        &log(WARN, "SOCKET $strSocketFile");

        my $oServer = logErrorResult(
            IO::Socket::UNIX->new(Type => SOCK_STREAM, Local => $strSocketFile, Listen => 1), ERROR_ARCHIVE_TIMEOUT,
            'unable to initialize ' . CMD_ARCHIVE_PUSH . " async process on socket: ${strSocketFile}");
        my $oSocket = $oServer->accept();

        &log(WARN, "CONNECTION FROM CLIENT");

        $self->processServer(new pgBackRest::Protocol::ArchivePushMinion($oSocket));

        $oSocket->close();
        $oServer->close();
        fileRemove($strSocketFile, true);
    }

    # # Continue with batch processing
    # if ($bBatch)
    # {
    #     # Start the async archive push
    #     logDebugMisc($strOperation, 'start async archive-push');
    #
    #     # Open the log file
    #     logFileSet(optionGet(OPTION_LOG_PATH) . '/' . optionGet(OPTION_STANZA) . '-archive-async');
    #
    #     # Call the archive_xfer function and continue to loop as long as there are files to process
    #     my $iLogTotal;
    #
    #     while (!defined($iLogTotal) || $iLogTotal > 0)
    #     {
    #         $iLogTotal = $self->xfer(optionGet(OPTION_SPOOL_PATH) . "/archive/" .
    #                                  optionGet(OPTION_STANZA) . "/out", $strStopFile);
    #
    #         if ($iLogTotal > 0)
    #         {
    #             logDebugMisc($strOperation, "transferred ${iLogTotal} WAL segment" .
    #                          ($iLogTotal > 1 ? 's' : '') . ', calling Archive->xfer() again');
    #         }
    #         else
    #         {
    #             logDebugMisc($strOperation, 'transfer found 0 WAL segments - exiting');
    #         }
    #     }
    #
    #     lockRelease();
    # }
    # elsif (defined($oException))
    # {
    #     confess $oException;
    # }

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'iResult', value => 0, trace => true}
    );
}

####################################################################################################################################
# processServer
####################################################################################################################################
sub processServer
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $oMinion,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '->processServer', \@_,
            {name => 'oMinion'},
        );

    $oMinion->process();

    # Return from function and log return values if any
    return logDebugReturn($strOperation);
}

####################################################################################################################################
# processClient
####################################################################################################################################
sub processClient
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $oMaster,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '->processServer', \@_,
            {name => 'oMinion'},
        );

    my $strWalSegment = $oMaster->cmdExecute(OP_ARCHIVE_PUSH_ASYNC, ['000000010000000100000001'], true);
    &log(WARN, "I AM CONNECTED: " . $strWalSegment);

    # Return from function and log return values if any
    return logDebugReturn($strOperation);
}

####################################################################################################################################
# readyList
####################################################################################################################################
sub readyList
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '->readyList', \@_,
        );

    # Read the ready files
    my $strWalStatusPath = "$self->{strWalPath}/archive_status";
    my @stryReadyFile = fileList($strWalStatusPath, '^.*\.ready$');

    &log(INFO, 'found ' . @stryReadyFile . ' .ready files');

    # Generate a list of new files
    my @stryNewReadyFile;

    foreach my $strReadyFile (@stryReadyFile)
    {
        $strReadyFile = substr($strReadyFile, 0, length($strReadyFile) - length('.ready'));

        if (!defined($self->{hWalState}{$strReadyFile}))
        {
            &log(INFO, "found new ready file ${strReadyFile}");
            push(@stryNewReadyFile, $strReadyFile);
            $self->{hWalState}{$strReadyFile} = false;
        }
    }

    return logDebugReturn
    (
        $strOperation,
        {name => 'stryWalFile', value => \@stryNewReadyFile, ref => true}
    );
}

####################################################################################################################################
# xfer
####################################################################################################################################
# sub xfer
# {
#     my $self = shift;
#
#     # Assign function parameters, defaults, and log debug info
#     my
#     (
#         $strOperation,
#         $strArchivePath,
#         $strStopFile
#     ) =
#         logDebugParam
#         (
#             __PACKAGE__ . '->xfer', \@_,
#             {name => 'strArchivePath'},
#             {name => 'strStopFile'}
#         );
#
#     # Create a local file object to read archive logs in the local store
#     my $oFile = new pgBackRest::File
#     (
#         optionGet(OPTION_STANZA),
#         optionGet(OPTION_REPO_PATH),
#         protocolGet(NONE)
#     );
#
#     # Load the archive manifest - all the files that need to be pushed
#     my $hManifest = $oFile->manifest(PATH_DB_ABSOLUTE, $strArchivePath);
#
#     # Get all the files to be transferred and calculate the total size
#     my @stryFile;
#     my $lFileSize = 0;
#     my $lFileTotal = 0;
#
#     foreach my $strFile (sort(keys(%{$hManifest})))
#     {
#         if ($strFile =~ "^[0-F]{24}(\\.partial){0,1}(-[0-f]{40})(\\.$oFile->{strCompressExtension}){0,1}\$" ||
#             $strFile =~ /^[0-F]{8}\.history$/ || $strFile =~ /^[0-F]{24}\.[0-F]{8}\.backup$/)
#         {
#             CORE::push(@stryFile, $strFile);
#
#             $lFileSize += $hManifest->{$strFile}{size};
#             $lFileTotal++;
#         }
#     }
#
#     if ($lFileTotal == 0)
#     {
#         logDebugMisc($strOperation, 'no WAL segments to archive');
#
#         return 0;
#     }
#     else
#     {
#         my $oException = undef;
#
#         eval
#         {
#             # Start backup test point
#             &log(TEST, TEST_ARCHIVE_PUSH_ASYNC_START);
#
#             # If the archive repo is remote create a new file object to do the copies
#             if (!isRepoLocal())
#             {
#                 $oFile = new pgBackRest::File
#                 (
#                     optionGet(OPTION_STANZA),
#                     optionGet(OPTION_REPO_PATH),
#                     protocolGet(BACKUP)
#                 );
#             }
#
#             # Modify process name to indicate async archiving
#             $0 = $^X . ' ' . $0 . " --stanza=" . optionGet(OPTION_STANZA) .
#                  "archive-push-async " . $stryFile[0] . '-' . $stryFile[scalar @stryFile - 1];
#
#             # Output files to be moved to backup
#             &log(INFO, "WAL segments to archive: total = ${lFileTotal}, size = " . fileSizeFormat($lFileSize));
#
#             # Transfer each file
#             foreach my $strFile (sort @stryFile)
#             {
#                 # Construct the archive filename to backup
#                 my $strArchiveFile = "${strArchivePath}/${strFile}";
#
#                 # Determine if the source file is already compressed
#                 my $bSourceCompressed = $strArchiveFile =~ "^.*\.$oFile->{strCompressExtension}\$" ? true : false;
#
#                 # Determine if this is an archive file (don't want to do compression or checksum on .backup files)
#                 my $bArchiveFile = basename($strFile) =~
#                     "^[0-F]{24}(\\.partial){0,1}(-[0-f]+){0,1}(\\.$oFile->{strCompressExtension}){0,1}\$" ? true : false;
#
#                 # Determine if this is a partial archive file
#                 my $bPartial = $bArchiveFile && basename($strFile) =~ /\.partial/ ? true : false;
#
#                 # Figure out whether the compression extension needs to be added or removed
#                 my $bDestinationCompress = $bArchiveFile && optionGet(OPTION_COMPRESS);
#                 my $strDestinationFile = basename($strFile);
#
#                 # Strip off existing checksum
#                 my $strAppendedChecksum = undef;
#
#                 if ($bArchiveFile)
#                 {
#                     $strAppendedChecksum = substr($strDestinationFile, $bPartial ? 33 : 25, 40);
#                     $strDestinationFile = substr($strDestinationFile, 0, $bPartial ? 32 : 24);
#                 }
#
#                 if ($bDestinationCompress)
#                 {
#                     $strDestinationFile .= ".$oFile->{strCompressExtension}";
#                 }
#
#                 logDebugMisc
#                 (
#                     $strOperation, undef,
#                     {name => 'strFile', value => $strFile},
#                     {name => 'bArchiveFile', value => $bArchiveFile},
#                     {name => 'bSourceCompressed', value => $bSourceCompressed},
#                     {name => 'bDestinationCompress', value => $bDestinationCompress}
#                 );
#
#                 # Check that there are no issues with pushing this WAL segment
#                 my $strArchiveId;
#                 my $strChecksum = undef;
#
#                 if ($bArchiveFile)
#                 {
#                     my ($strDbVersion, $ullDbSysId) = $self->walInfo($strArchiveFile);
#                     ($strArchiveId, $strChecksum) = $self->pushCheck(
#                         $oFile, substr(basename($strArchiveFile), 0, 24), $bPartial, $strArchiveFile, $strDbVersion, $ullDbSysId);
#                 }
#                 else
#                 {
#                     $strArchiveId = $self->getCheck($oFile);
#                 }
#
#                 # Only copy the WAL segment if checksum is not defined.  If checksum is defined it means that the WAL segment
#                 # already exists in the repository with the same checksum (else there would have been an error on checksum
#                 # mismatch).
#                 if (!defined($strChecksum))
#                 {
#                     # Copy the archive file
#                     my ($bResult, $strCopyChecksum) = $oFile->copy(
#                         PATH_DB_ABSOLUTE, $strArchiveFile,          # Source path/file
#                         PATH_BACKUP_ARCHIVE,                        # Destination path
#                         "${strArchiveId}/${strDestinationFile}",    # Destination file
#                         $bSourceCompressed,                         # Source is not compressed
#                         $bDestinationCompress,                      # Destination compress is configurable
#                         undef, undef, undef,                        # Unused params
#                         true,                                       # Create path if it does not exist
#                         undef, undef,                               # Unused params
#                         true,                                       # Append checksum
#                         optionGet(OPTION_REPO_SYNC));               # Sync path if set
#
#                     # If appended checksum does not equal copy checksum
#                     if (defined($strAppendedChecksum) && $strAppendedChecksum ne $strCopyChecksum)
#                     {
#                         confess &log(
#                             ERROR,
#                             "archive ${strArchiveFile} appended checksum ${strAppendedChecksum} does not match" .
#                                 " copy checksum ${strCopyChecksum}", ERROR_ARCHIVE_MISMATCH);
#                     }
#                 }
#
#                 #  Remove the source archive file
#                 unlink($strArchiveFile)
#                     or confess &log(ERROR, "copied ${strArchiveFile} to archive successfully but unable to remove it locally.  " .
#                                            'This file will need to be cleaned up manually.  If the problem persists, check if ' .
#                                            CMD_ARCHIVE_PUSH . ' is being run with different permissions in different contexts.');
#
#                 # Remove the copied segment from the total size
#                 $lFileSize -= $hManifest->{$strFile}{size};
#             }
#
#             return true;
#         }
#         or do
#         {
#             $oException = $EVAL_ERROR;
#         };
#
#         # Create a stop file if the archive store exceeds the max even after xfer
#         if (optionTest(OPTION_ARCHIVE_MAX_MB))
#         {
#             my $iArchiveMaxMB = optionGet(OPTION_ARCHIVE_MAX_MB);
#
#             if ($iArchiveMaxMB < int($lFileSize / 1024 / 1024))
#             {
#                 &log(ERROR, "local archive queue has exceeded limit of ${iArchiveMaxMB}MB" .
#                             " - WAL segments will be discarded until the stop file (${strStopFile}) is removed");
#
#                 filePathCreate(dirname($strStopFile), '0770');
#
#                 my $hStopFile;
#                 open($hStopFile, '>', $strStopFile)
#                     or confess &log(ERROR, "unable to create stop file file ${strStopFile}");
#                 close($hStopFile);
#             }
#         }
#
#         # If there was an exception before throw it now
#         if ($oException)
#         {
#             confess $oException;
#         }
#     }
#
#     # Return from function and log return values if any
#     return logDebugReturn
#     (
#         $strOperation,
#         {name => 'lFileTotal', value => $lFileTotal}
#     );
# }

1;
