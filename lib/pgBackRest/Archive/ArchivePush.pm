####################################################################################################################################
# ARCHIVE PUSH MODULE
####################################################################################################################################
package pgBackRest::Archive::ArchivePush;
use parent 'pgBackRest::Archive::Archive';

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
# PostgreSQL WAL magic
####################################################################################################################################
my $oWalMagicHash =
{
    hex('0xD062') => PG_VERSION_83,
    hex('0xD063') => PG_VERSION_84,
    hex('0xD064') => PG_VERSION_90,
    hex('0xD066') => PG_VERSION_91,
    hex('0xD071') => PG_VERSION_92,
    hex('0xD075') => PG_VERSION_93,
    hex('0xD07E') => PG_VERSION_94,
    hex('0xD087') => PG_VERSION_95,
    hex('0xD093') => PG_VERSION_96,
};

####################################################################################################################################
# process
####################################################################################################################################
sub process
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my ($strOperation) = logDebugParam(__PACKAGE__ . '->process');

    # Make sure the archive push command happens on the db side
    if (!isDbLocal())
    {
        confess &log(ERROR, CMD_ARCHIVE_PUSH . ' operation must run on the db host');
    }

    # Error if no WAL segment is defined
    my $strWalSegment = $ARGV[1];

    if (!defined($strWalSegment))
    {
        confess &log(ERROR, 'wal segment to push required', ERROR_PARAM_REQUIRED);
    }

    my $strWalSegmentFile = basename($strWalSegment);

    # Check for a stop lock
    lockStopTest();

    # Only do async archiving when this file being archived is a WAL segment, otherwise do it synchronously
    my $bArchiveAsync = optionGet(OPTION_ARCHIVE_ASYNC) && $strWalSegmentFile =~ '^[0-F]{24}$';

    # If logging locally then create the stop archiving file name
    # my $strStopFile;
    #
    # if ($bArchiveAsync)
    # {
    #     $strStopFile = optionGet(OPTION_SPOOL_PATH) . '/stop/' . optionGet(OPTION_STANZA) . "-archive.stop";
    # }

    # If an archive file is defined, then push it
    my $oException = undef;

    # Start the async process and wait for WAL to complete
    if ($bArchiveAsync)
    {
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

        &log(INFO, "push WAL segment ${strWalSegmentFile} asynchronously");

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

            my $oMaster = new pgBackRest::Protocol::ArchivePushMaster($oSocket);
            my $strWalSegment = '[undefined]';

            $strWalSegment = $oMaster->cmdExecute(OP_ARCHIVE_PUSH_ASYNC, ['000000010000000100000001'], true);

            # my $oConn = new pgBackRest::Protocol::IO(
            #     $oClient, $oClient, undef, undef, 'socket-1', 5, OPTION_DEFAULT_BUFFER_SIZE);

            &log(WARN, "I AM CONNECTED: " . $strWalSegment);
        }
        else
        {
            chdir '/'
                or confess "chdir() failed: $!";

            # close stdin/stdout
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

            my $oMinion = new pgBackRest::Protocol::ArchivePushMinion($oSocket);
            $oMinion->process();

            $oSocket->close();
            $oServer->close();
            fileRemove($strSocketFile, true);
        }
    }
    # Else push synchronously
    else
    {
        $self->push($ARGV[1]);
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
# walInfo
#
# Retrieve information such as db version and system identifier from a WAL segment.
####################################################################################################################################
sub walInfo
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $strWalFile,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '->walInfo', \@_,
            {name => 'strWalFile'}
        );

    # Open the WAL segment and read magic number
    #-------------------------------------------------------------------------------------------------------------------------------
    my $hFile;
    my $tBlock;

    sysopen($hFile, $strWalFile, O_RDONLY)
        or confess &log(ERROR, "unable to open ${strWalFile}", ERROR_FILE_OPEN);

    # Read magic
    sysread($hFile, $tBlock, 2) == 2
        or confess &log(ERROR, "unable to read xlog magic");

    my $iMagic = unpack('S', $tBlock);

    # Map the WAL magic number to the version of PostgreSQL.
    #
    # The magic number can be found in src/include/access/xlog_internal.h The offset can be determined by counting bytes in the
    # XLogPageHeaderData struct, though this value rarely changes.
    #-------------------------------------------------------------------------------------------------------------------------------
    my $strDbVersion = $$oWalMagicHash{$iMagic};

    if (!defined($strDbVersion))
    {
        confess &log(ERROR, "unexpected WAL magic 0x" . sprintf("%X", $iMagic) . "\n" .
                     'HINT: is this version of PostgreSQL supported?',
                     ERROR_VERSION_NOT_SUPPORTED);
    }

    # Map the WAL PostgreSQL version to the system identifier offset.  The offset can be determined by counting bytes in the
    # XLogPageHeaderData struct, though this value rarely changes.
    #-------------------------------------------------------------------------------------------------------------------------------
    my $iSysIdOffset = $strDbVersion >= PG_VERSION_93 ? PG_WAL_SYSTEM_ID_OFFSET_GTE_93 : PG_WAL_SYSTEM_ID_OFFSET_LT_93;

    # Check flags to be sure the long header is present (this is an extra check to be sure the system id exists)
    #-------------------------------------------------------------------------------------------------------------------------------
    sysread($hFile, $tBlock, 2) == 2
        or confess &log(ERROR, "unable to read xlog info");

    my $iFlag = unpack('S', $tBlock);

    # Make sure that the long header is present or there won't be a system id
    $iFlag & 2
        or confess &log(ERROR, "expected long header in flags " . sprintf("%x", $iFlag));

    # Get the system id
    #-------------------------------------------------------------------------------------------------------------------------------
    sysseek($hFile, $iSysIdOffset, SEEK_CUR)
        or confess &log(ERROR, "unable to read padding");

    sysread($hFile, $tBlock, 8) == 8
        or confess &log(ERROR, "unable to read database system identifier");

    length($tBlock) == 8
        or confess &log(ERROR, "block is incorrect length");

    close($hFile);

    my $ullDbSysId = unpack('Q', $tBlock);

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'strDbVersion', value => $strDbVersion},
        {name => 'ullDbSysId', value => $ullDbSysId}
    );
}

####################################################################################################################################
# push
####################################################################################################################################
sub push
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $strSourceFile,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '->push', \@_,
            {name => 'strSourceFile'},
        );

    # Create the file object
    my $oFile = new pgBackRest::File
    (
        optionGet(OPTION_STANZA),
        optionGet(OPTION_REPO_PATH),
        protocolGet(BACKUP)
    );

    # Construct absolute path to the WAL file when it is relative
    $strSourceFile = walPath($strSourceFile, optionGet(OPTION_DB_PATH, false), commandGet());

    # Get the destination file
    my $strDestinationFile = basename($strSourceFile);

    # Get the compress flag
    my $bCompress = optionGet(OPTION_COMPRESS);

    # Determine if this is an archive file (don't do compression or checksum on .backup, .history, etc.)
    my $bArchiveFile = basename($strSourceFile) =~ /^[0-F]{24}(\.partial){0,1}$/ ? true : false;

    # Determine if this is a partial archive file
    my $bPartial = $bArchiveFile && basename($strSourceFile) =~ /\.partial/ ? true : false;

    # Check that there are no issues with pushing this WAL segment
    my $strArchiveId;
    my $strChecksum = undef;

    if ($bArchiveFile)
    {
        my ($strDbVersion, $ullDbSysId) = $self->walInfo($strSourceFile);
        ($strArchiveId, $strChecksum) = $self->pushCheck(
            $oFile, substr(basename($strSourceFile), 0, 24), $bPartial, $strSourceFile, $strDbVersion, $ullDbSysId);
    }
    else
    {
        $strArchiveId = $self->getCheck($oFile);
    }

    # Only copy the WAL segment if checksum is not defined.  If checksum is defined it means that the WAL segment already exists
    # in the repository with the same checksum (else there would have been an error on checksum mismatch).
    if (!defined($strChecksum))
    {
        # Append compression extension
        if ($bArchiveFile && $bCompress)
        {
            $strDestinationFile .= '.' . $oFile->{strCompressExtension};
        }

        # Copy the WAL segment
        $oFile->copy(
            PATH_DB_ABSOLUTE, $strSourceFile,                       # Source type/file
            PATH_BACKUP_ARCHIVE,                                    # Destination type
            "${strArchiveId}/${strDestinationFile}",                # Destination file
            false,                                                  # Source is not compressed
            $bArchiveFile && $bCompress,                            # Destination compress is configurable
            undef, undef, undef,                                    # Unused params
            true,                                                   # Create path if it does not exist
            undef, undef,                                           # Default User and group
            $bArchiveFile,                                          # Append checksum if archive file
            optionGet(OPTION_REPO_SYNC));                           # Sync repo directories?
    }

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation
    );
}

####################################################################################################################################
# pushCheck
####################################################################################################################################
sub pushCheck
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $oFile,
        $strWalSegment,
        $bPartial,
        $strWalFile,
        $strDbVersion,
        $ullDbSysId,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '->pushCheck', \@_,
            {name => 'oFile'},
            {name => 'strWalSegment'},
            {name => 'bPartial'},
            {name => 'strWalFile', required => false},
            {name => 'strDbVersion'},
            {name => 'ullDbSysId'},
        );

    # Set operation and debug strings
    my $strArchiveId;
    my $strChecksum;

    if ($oFile->isRemote(PATH_BACKUP_ARCHIVE))
    {
        # Execute the command
        ($strArchiveId, $strChecksum) = $oFile->{oProtocol}->cmdExecute(
            OP_ARCHIVE_PUSH_CHECK, [$strWalSegment, $bPartial, undef, $strDbVersion, $ullDbSysId], true);
    }
    else
    {
        # If the info file exists check db version and system-id else error
        $strArchiveId = (new pgBackRest::Archive::ArchiveInfo($oFile->pathGet(PATH_BACKUP_ARCHIVE)))->check(
            $strDbVersion, $ullDbSysId);

        # Check if the WAL segment already exists in the archive
        $strChecksum = walFind($oFile, $strArchiveId, $strWalSegment, $bPartial);

        if (defined($strChecksum))
        {
            $strChecksum = substr($strChecksum, $bPartial ? 33 : 25, 40);
        }
    }

    if (defined($strChecksum) && defined($strWalFile))
    {
        my $strChecksumNew = $oFile->hash(PATH_DB_ABSOLUTE, $strWalFile);

        if ($strChecksumNew ne $strChecksum)
        {
            confess &log(ERROR, "WAL segment ${strWalSegment}" . ($bPartial ? '.partial' : '') .
                                ' already exists in the archive', ERROR_ARCHIVE_DUPLICATE);
        }

        &log(WARN, "WAL segment ${strWalSegment}" . ($bPartial ? '.partial' : '') .
                   " already exists in the archive with the same checksum\n" .
                   "HINT: this is valid in some recovery scenarios but may also indicate a problem");
    }

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'strArchiveId', value => $strArchiveId},
        {name => 'strChecksum', value => $strChecksum}
    );
}

####################################################################################################################################
# xfer
####################################################################################################################################
sub xfer
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $strArchivePath,
        $strStopFile
    ) =
        logDebugParam
        (
            __PACKAGE__ . '->xfer', \@_,
            {name => 'strArchivePath'},
            {name => 'strStopFile'}
        );

    # Create a local file object to read archive logs in the local store
    my $oFile = new pgBackRest::File
    (
        optionGet(OPTION_STANZA),
        optionGet(OPTION_REPO_PATH),
        protocolGet(NONE)
    );

    # Load the archive manifest - all the files that need to be pushed
    my $hManifest = $oFile->manifest(PATH_DB_ABSOLUTE, $strArchivePath);

    # Get all the files to be transferred and calculate the total size
    my @stryFile;
    my $lFileSize = 0;
    my $lFileTotal = 0;

    foreach my $strFile (sort(keys(%{$hManifest})))
    {
        if ($strFile =~ "^[0-F]{24}(\\.partial){0,1}(-[0-f]{40})(\\.$oFile->{strCompressExtension}){0,1}\$" ||
            $strFile =~ /^[0-F]{8}\.history$/ || $strFile =~ /^[0-F]{24}\.[0-F]{8}\.backup$/)
        {
            CORE::push(@stryFile, $strFile);

            $lFileSize += $hManifest->{$strFile}{size};
            $lFileTotal++;
        }
    }

    if ($lFileTotal == 0)
    {
        logDebugMisc($strOperation, 'no WAL segments to archive');

        return 0;
    }
    else
    {
        my $oException = undef;

        eval
        {
            # Start backup test point
            &log(TEST, TEST_ARCHIVE_PUSH_ASYNC_START);

            # If the archive repo is remote create a new file object to do the copies
            if (!isRepoLocal())
            {
                $oFile = new pgBackRest::File
                (
                    optionGet(OPTION_STANZA),
                    optionGet(OPTION_REPO_PATH),
                    protocolGet(BACKUP)
                );
            }

            # Modify process name to indicate async archiving
            $0 = $^X . ' ' . $0 . " --stanza=" . optionGet(OPTION_STANZA) .
                 "archive-push-async " . $stryFile[0] . '-' . $stryFile[scalar @stryFile - 1];

            # Output files to be moved to backup
            &log(INFO, "WAL segments to archive: total = ${lFileTotal}, size = " . fileSizeFormat($lFileSize));

            # Transfer each file
            foreach my $strFile (sort @stryFile)
            {
                # Construct the archive filename to backup
                my $strArchiveFile = "${strArchivePath}/${strFile}";

                # Determine if the source file is already compressed
                my $bSourceCompressed = $strArchiveFile =~ "^.*\.$oFile->{strCompressExtension}\$" ? true : false;

                # Determine if this is an archive file (don't want to do compression or checksum on .backup files)
                my $bArchiveFile = basename($strFile) =~
                    "^[0-F]{24}(\\.partial){0,1}(-[0-f]+){0,1}(\\.$oFile->{strCompressExtension}){0,1}\$" ? true : false;

                # Determine if this is a partial archive file
                my $bPartial = $bArchiveFile && basename($strFile) =~ /\.partial/ ? true : false;

                # Figure out whether the compression extension needs to be added or removed
                my $bDestinationCompress = $bArchiveFile && optionGet(OPTION_COMPRESS);
                my $strDestinationFile = basename($strFile);

                # Strip off existing checksum
                my $strAppendedChecksum = undef;

                if ($bArchiveFile)
                {
                    $strAppendedChecksum = substr($strDestinationFile, $bPartial ? 33 : 25, 40);
                    $strDestinationFile = substr($strDestinationFile, 0, $bPartial ? 32 : 24);
                }

                if ($bDestinationCompress)
                {
                    $strDestinationFile .= ".$oFile->{strCompressExtension}";
                }

                logDebugMisc
                (
                    $strOperation, undef,
                    {name => 'strFile', value => $strFile},
                    {name => 'bArchiveFile', value => $bArchiveFile},
                    {name => 'bSourceCompressed', value => $bSourceCompressed},
                    {name => 'bDestinationCompress', value => $bDestinationCompress}
                );

                # Check that there are no issues with pushing this WAL segment
                my $strArchiveId;
                my $strChecksum = undef;

                if ($bArchiveFile)
                {
                    my ($strDbVersion, $ullDbSysId) = $self->walInfo($strArchiveFile);
                    ($strArchiveId, $strChecksum) = $self->pushCheck(
                        $oFile, substr(basename($strArchiveFile), 0, 24), $bPartial, $strArchiveFile, $strDbVersion, $ullDbSysId);
                }
                else
                {
                    $strArchiveId = $self->getCheck($oFile);
                }

                # Only copy the WAL segment if checksum is not defined.  If checksum is defined it means that the WAL segment
                # already exists in the repository with the same checksum (else there would have been an error on checksum
                # mismatch).
                if (!defined($strChecksum))
                {
                    # Copy the archive file
                    my ($bResult, $strCopyChecksum) = $oFile->copy(
                        PATH_DB_ABSOLUTE, $strArchiveFile,          # Source path/file
                        PATH_BACKUP_ARCHIVE,                        # Destination path
                        "${strArchiveId}/${strDestinationFile}",    # Destination file
                        $bSourceCompressed,                         # Source is not compressed
                        $bDestinationCompress,                      # Destination compress is configurable
                        undef, undef, undef,                        # Unused params
                        true,                                       # Create path if it does not exist
                        undef, undef,                               # Unused params
                        true,                                       # Append checksum
                        optionGet(OPTION_REPO_SYNC));               # Sync path if set

                    # If appended checksum does not equal copy checksum
                    if (defined($strAppendedChecksum) && $strAppendedChecksum ne $strCopyChecksum)
                    {
                        confess &log(
                            ERROR,
                            "archive ${strArchiveFile} appended checksum ${strAppendedChecksum} does not match" .
                                " copy checksum ${strCopyChecksum}", ERROR_ARCHIVE_MISMATCH);
                    }
                }

                #  Remove the source archive file
                unlink($strArchiveFile)
                    or confess &log(ERROR, "copied ${strArchiveFile} to archive successfully but unable to remove it locally.  " .
                                           'This file will need to be cleaned up manually.  If the problem persists, check if ' .
                                           CMD_ARCHIVE_PUSH . ' is being run with different permissions in different contexts.');

                # Remove the copied segment from the total size
                $lFileSize -= $hManifest->{$strFile}{size};
            }

            return true;
        }
        or do
        {
            $oException = $EVAL_ERROR;
        };

        # Create a stop file if the archive store exceeds the max even after xfer
        if (optionTest(OPTION_ARCHIVE_MAX_MB))
        {
            my $iArchiveMaxMB = optionGet(OPTION_ARCHIVE_MAX_MB);

            if ($iArchiveMaxMB < int($lFileSize / 1024 / 1024))
            {
                &log(ERROR, "local archive queue has exceeded limit of ${iArchiveMaxMB}MB" .
                            " - WAL segments will be discarded until the stop file (${strStopFile}) is removed");

                filePathCreate(dirname($strStopFile), '0770');

                my $hStopFile;
                open($hStopFile, '>', $strStopFile)
                    or confess &log(ERROR, "unable to create stop file file ${strStopFile}");
                close($hStopFile);
            }
        }

        # If there was an exception before throw it now
        if ($oException)
        {
            confess $oException;
        }
    }

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'lFileTotal', value => $lFileTotal}
    );
}

1;
