####################################################################################################################################
# PROTOCOL SOCKET IO MODULE
####################################################################################################################################
package pgBackRest::Protocol::IO::SocketIO;
use parent 'pgBackRest::Protocol::IO::IO';

use strict;
use warnings FATAL => qw(all);
use Carp qw(confess);
use English '-no_match_vars';

use pgBackRest::Common::Exception;
use pgBackRest::Common::Log;

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
        $oSocket,                                                   # Socket
        $strId,                                                     # Id for messages
        $iProtocolTimeout,                                          # Protocol timeout
        $iBufferMax,                                                # Maximum buffer size
    ) =
        logDebugParam
        (
            __PACKAGE__ . '->new', \@_,
            {name => 'hndIn', required => false, trace => true},
            {name => 'hndOut', required => false, trace => true},
            {name => 'hndErr', required => false, trace => true},
            {name => 'pId', required => false, trace => true},
            {name => 'strId', required => false, trace => true},
            {name => 'iProtocolTimeout', trace => true},
            {name => 'iBufferMax', trace => true}
        );

    my $self = $class->SUPER::new($oSocket, $oSocket, undef, $iProtocolTimeout, $iBufferMax);

    $self->{oSocket} = $oSocket;

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'self', value => $self}
    );
}

####################################################################################################################################
# Get socket
####################################################################################################################################
sub socket()
{
    my $self = shift;

    return $self->{oSocket};
}

1;
