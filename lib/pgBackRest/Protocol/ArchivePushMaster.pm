####################################################################################################################################
# PROTOCOL ARCHIVE PUSH MASTER MODULE
####################################################################################################################################
package pgBackRest::Protocol::ArchivePushMaster;
use parent 'pgBackRest::Protocol::SocketMaster';

use strict;
use warnings FATAL => qw(all);
use Carp qw(confess);

use pgBackRest::BackupFile;
use pgBackRest::Common::Log;
use pgBackRest::Config::Config;
use pgBackRest::Protocol::CommandMaster;
use pgBackRest::Protocol::Common;

####################################################################################################################################
# CONSTRUCTOR
####################################################################################################################################
sub new
{
    my $class = shift;

    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $oSocket,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '->new', \@_,
            {name => 'oSocket'},
        );

    # Init object
    my $self = $class->SUPER::new(
        NONE, CMD_ARCHIVE_PUSH, CMD_ARCHIVE_PUSH, $oSocket, optionGet(OPTION_BUFFER_SIZE), optionGet(OPTION_COMPRESS_LEVEL),
        optionGet(OPTION_COMPRESS_LEVEL_NETWORK), optionGet(OPTION_PROTOCOL_TIMEOUT));
    bless $self, $class;

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'self', value => $self}
    );
}

1;
