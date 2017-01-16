####################################################################################################################################
# ARCHIVE COMMON MODULE
####################################################################################################################################
package pgBackRest::Archive::ArchiveCommon;

use strict;
use warnings FATAL => qw(all);
use Carp qw(confess);

use Exporter qw(import);
    our @EXPORT = qw();
use File::Basename qw(dirname);

use pgBackRest::Db;
use pgBackRest::DbVersion;
use pgBackRest::Common::Exception;
use pgBackRest::Common::Log;
use pgBackRest::Common::Wait;
use pgBackRest::Config::Config;
use pgBackRest::File;
use pgBackRest::FileCommon;
use pgBackRest::Protocol::Common;

####################################################################################################################################
# RegEx constants
####################################################################################################################################
use constant REGEX_ARCHIVE_DIR_DB_VERSION                           => '^[0-9]+\.[0-9]+-[0-9]+$';
    push @EXPORT, qw(REGEX_ARCHIVE_DIR_DB_VERSION);
use constant REGEX_ARCHIVE_DIR_WAL                                  => '^[0-F]{16}$';
    push @EXPORT, qw(REGEX_ARCHIVE_DIR_WAL);

####################################################################################################################################
# PostgreSQL WAL system id offset
####################################################################################################################################
use constant PG_WAL_SYSTEM_ID_OFFSET_GTE_93                         => 20;
    push @EXPORT, qw(PG_WAL_SYSTEM_ID_OFFSET_GTE_93);
use constant PG_WAL_SYSTEM_ID_OFFSET_LT_93                          => 12;
    push @EXPORT, qw(PG_WAL_SYSTEM_ID_OFFSET_LT_93);

####################################################################################################################################
# constructor
####################################################################################################################################
sub new
{
    my $class = shift;          # Class name

    # Assign function parameters, defaults, and log debug info
    my ($strOperation) = logDebugParam(__PACKAGE__ . '->new');

    # Create the class hash
    my $self = {};
    bless $self, $class;

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'self', value => $self}
    );
}

####################################################################################################################################
# getCheck
####################################################################################################################################
sub getCheck
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $oFile,
        $strDbVersion,
        $ullDbSysId
    ) =
        logDebugParam
    (
        __PACKAGE__ . '->getCheck', \@_,
        {name => 'oFile'},
        {name => 'strDbVersion', required => false},
        {name => 'ullDbSysId', required => false}
    );

    my $strArchiveId;

    # If the dbVersion/dbSysId are not passed, then we need to retrieve the database information
    if (!defined($strDbVersion) || !defined($ullDbSysId) )
    {
        # get DB info for comparison
        ($strDbVersion, my $iControlVersion, my $iCatalogVersion, $ullDbSysId) = dbMasterGet()->info();
    }

    if ($oFile->isRemote(PATH_BACKUP_ARCHIVE))
    {
        $strArchiveId = $oFile->{oProtocol}->cmdExecute(OP_ARCHIVE_GET_CHECK, [$strDbVersion, $ullDbSysId], true);
    }
    else
    {
        # check that the archive info is compatible with the database
        $strArchiveId =
            (new pgBackRest::Archive::ArchiveInfo($oFile->pathGet(PATH_BACKUP_ARCHIVE), true))->check($strDbVersion, $ullDbSysId);
    }

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'strArchiveId', value => $strArchiveId, trace => true}
    );
}

####################################################################################################################################
# lsnNormalize
#
# Generates a normalized form from an LSN that can be used for comparison.
####################################################################################################################################
sub lsnNormalize
{
    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $strLsn,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '::lsnFile', \@_,
            {name => 'strLsn', trace => true},
        );

    # Split the LSN into major and minor parts
    my @stryLsnSplit = split('/', $strLsn);

    if (@stryLsnSplit != 2)
    {
        confess &log(ASSERT, "invalid lsn ${strLsn}");
    }

    my $strLsnNormal = uc(sprintf("%08s%08s", $stryLsnSplit[0], $stryLsnSplit[1]));

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'strLsnNormal', value => $strLsnNormal, trace => true}
    );

}

push @EXPORT, qw(lsnNormalize);

####################################################################################################################################
# lsnFileRange
#
# Generates a range of WAL filenames given the start and stop LSN.  For pre-9.3 databases, use bSkipFF to exclude the FF that
# prior versions did not generate.
####################################################################################################################################
sub lsnFileRange
{
    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $strLsnStart,
        $strLsnStop,
        $strDbVersion,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '::lsnFileRange', \@_,
            {name => 'strLsnStart'},
            {name => 'strLsnStop'},
            {name => '$strDbVersion'},
        );

    # Working variables
    my @stryArchive;
    my $iArchiveIdx = 0;
    my $bSkipFF = $strDbVersion < PG_VERSION_93;

    # Iterate through all archive logs between start and stop
    my @stryArchiveSplit = split('/', $strLsnStart);
    my $iStartMajor = hex($stryArchiveSplit[0]);
    my $iStartMinor = hex(substr(sprintf("%08s", $stryArchiveSplit[1]), 0, 2));

    @stryArchiveSplit = split('/', $strLsnStop);
    my $iStopMajor = hex($stryArchiveSplit[0]);
    my $iStopMinor = hex(substr(sprintf("%08s", $stryArchiveSplit[1]), 0, 2));

    $stryArchive[$iArchiveIdx] = uc(sprintf("%08x%08x", $iStartMajor, $iStartMinor));
    $iArchiveIdx += 1;

    while (!($iStartMajor == $iStopMajor && $iStartMinor == $iStopMinor))
    {
        $iStartMinor += 1;

        if ($bSkipFF && $iStartMinor == 255 || !$bSkipFF && $iStartMinor == 256)
        {
            $iStartMajor += 1;
            $iStartMinor = 0;
        }

        $stryArchive[$iArchiveIdx] = uc(sprintf("%08x%08x", $iStartMajor, $iStartMinor));
        $iArchiveIdx += 1;
    }

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'stryWalFileName', value => \@stryArchive}
    );
}

push @EXPORT, qw(lsnFileRange);

####################################################################################################################################
# walSegmentFind
#
# Returns the filename in the archive of a WAL segment.  Optionally, a wait time can be specified.  In this case an error will be
# thrown when the WAL segment is not found.  If the same WAL segment with multiple checksums is found then error.
####################################################################################################################################
sub walSegmentFind
{
    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $strArchivePath,
        $strWalSegment,
        $iWaitSeconds,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '::walFind', \@_,
            {name => 'strArchivePath'},
            {name => 'strWalSegment'},
            {name => 'iWaitSeconds', required => false},
        );

    # Error if not a segment
    my $bTimeline = $strWalSegment =~ /^[0-F]{16}$/ ? false : true;

    if ($bTimeline && !walIsSegment($strWalSegment))
    {
        confess &log(ERROR, "${strWalSegment} is not a WAL segment", ERROR_ASSERT);
    }

    # Test if partial
    my $bPartial = $strWalSegment =~ /\.partial$/ ? true : false;

    # Loop and wait for file to appear
    my $oWait = waitInit($iWaitSeconds);
    my @stryWalFileName;

    do
    {
        # If the WAL segment includes the timeline then use it, otherwise contruct a regexp with the major WAL part to find paths
        # where the wal could be found.
        my @stryTimelineMajor;

        if ($bTimeline)
        {
            @stryTimelineMajor = (substr($strWalSegment, 0, 16));
        }
        else
        {
            @stryTimelineMajor = fileList($strArchivePath, '[0-F]{8}' . substr($strWalSegment, 0, 8), undef, true);
        }

        # Search each timelin/major path
        foreach my $strTimelineMajor (@stryTimelineMajor)
        {
            # Construct the name of the WAL segment to find
            my $strWalSegmentFind = $bTimeline ? $strWalSegment : $strTimelineMajor . substr($strWalSegment, 8, 16);

            # Get the name of the requested WAL segment (may have hash info and compression extension)
            push(@stryWalFileName, fileList(
                "${strArchivePath}/${strTimelineMajor}",
                "^${strWalSegmentFind}" . ($bPartial ? "\\.partial" : '') . "-[0-f]{40}(\\." . COMPRESS_EXT . "){0,1}\$",
                undef, true));
        }
    }
    while (@stryWalFileName == 0 && waitMore($oWait));

    # If there is more than one matching archive file then there is a serious issue - either a bug in the archiver or the user has
    # copied files around or removed archive.info.
    if (@stryWalFileName > 1)
    {
        confess &log(ERROR,
            "duplicates found in archive for WAL segment " . ($bTimeline ? $strWalSegment : "XXXXXXXX${strWalSegment}") . ': ' .
            join(', ', @stryWalFileName), ERROR_ARCHIVE_DUPLICATE);
    }

    # If waiting and no WAL segment was found then throw an error
    if (@stryWalFileName == 0 && defined($iWaitSeconds))
    {
        confess &log(ERROR, "could not find WAL segment ${strWalSegment} after ${iWaitSeconds} second(s)", ERROR_ARCHIVE_TIMEOUT);
    }

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'strWalFileName', value => $stryWalFileName[0]}
    );
}

push @EXPORT, qw(walSegmentFind);

####################################################################################################################################
# walPath
#
# Generates the location of the pg_xlog directory using a relative xlog path and the supplied db path.
####################################################################################################################################
sub walPath
{
    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $strWalFile,
        $strDbPath,
        $strCommand,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '::walPath', \@_,
            {name => 'strWalFile', trace => true},
            {name => 'strDbPath', trace => true, required => false},
            {name => 'strCommand', trace => true},
        );

    if (index($strWalFile, '/') != 0)
    {
        if (!defined($strDbPath))
        {
            confess &log(ERROR,
                "option 'db-path' must be specified when relative xlog paths are used\n" .
                "HINT: Is \%f passed to ${strCommand} instead of \%p?\n" .
                "HINT: PostgreSQL may pass relative paths even with \%p depending on the environment.",
                ERROR_OPTION_REQUIRED);
        }

        $strWalFile = "${strDbPath}/${strWalFile}";
    }

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'strWalFile', value => $strWalFile, trace => true}
    );

}

push @EXPORT, qw(walPath);

####################################################################################################################################
# walIsSegment
#
# Is the file a segment or some other file (e.g. .history, .backup, etc).
####################################################################################################################################
sub walIsSegment
{
    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $strWalFile,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '::walIsSegment', \@_,
            {name => 'strWalFile', trace => true},
        );

    return $strWalFile =~ /^[0-F]{24}(\.partial){0,1}$/ ? true : false;
}

push @EXPORT, qw(walIsSegment);

####################################################################################################################################
# walIsPartial
#
# Is the file a segment and partial.
####################################################################################################################################
sub walIsPartial
{
    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $strWalFile,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '::walIsPartial', \@_,
            {name => 'strWalFile', trace => true},
        );

    return walIsSegment($strWalFile) && $strWalFile =~ /\.partial$/ ? true : false;
}

push @EXPORT, qw(walIsSegment);

1;
