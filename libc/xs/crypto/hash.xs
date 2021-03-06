####################################################################################################################################
# Cryptographic Hashes Perl Exports
#
# XS wrapper for functions in cipher/hash.c.
####################################################################################################################################

MODULE = pgBackRest::LibC PACKAGE = pgBackRest::LibC::Crypto::Hash

####################################################################################################################################
pgBackRest::LibC::Crypto::Hash
new(class, type)
    const char *class
    const char *type
CODE:
    RETVAL = NULL;

    // Don't warn when class param is used
    (void)class;

    MEM_CONTEXT_XS_NEW_BEGIN("cryptoHashXs")
    {
        RETVAL = memNew(sizeof(CryptoHashXs));
        RETVAL->memContext = MEM_COMTEXT_XS();
        RETVAL->pxPayload = cryptoHashNew(strNew(type));
    }
    MEM_CONTEXT_XS_NEW_END();
OUTPUT:
    RETVAL

####################################################################################################################################
void
process(self, message)
    pgBackRest::LibC::Crypto::Hash self
    SV *message
CODE:
    MEM_CONTEXT_XS_TEMP_BEGIN()
    {
        STRLEN messageSize;
        const unsigned char *messagePtr = (const unsigned char *)SvPV(message, messageSize);

        cryptoHashProcessC(self->pxPayload, messagePtr, messageSize);
    }
    MEM_CONTEXT_XS_TEMP_END();

####################################################################################################################################
SV *
result(self)
    pgBackRest::LibC::Crypto::Hash self
CODE:
    RETVAL = NULL;

    MEM_CONTEXT_XS_TEMP_BEGIN()
    {
        String *hash = bufHex(cryptoHash(self->pxPayload));

        RETVAL = newSV(strSize(hash));
        SvPOK_only(RETVAL);
        strcpy((char *)SvPV_nolen(RETVAL), strPtr(hash));
        SvCUR_set(RETVAL, strSize(hash));
    }
    MEM_CONTEXT_XS_TEMP_END();
OUTPUT:
    RETVAL

####################################################################################################################################
void
DESTROY(self)
    pgBackRest::LibC::Crypto::Hash self
CODE:
    MEM_CONTEXT_XS_DESTROY(self->memContext);

MODULE = pgBackRest::LibC PACKAGE = pgBackRest::LibC

####################################################################################################################################
SV *
cryptoHashOne(type, message)
    const char *type
    SV *message
CODE:
    RETVAL = NULL;

    MEM_CONTEXT_XS_TEMP_BEGIN()
    {
        STRLEN messageSize;
        const unsigned char *messagePtr = (const unsigned char *)SvPV(message, messageSize);

        String *hash = bufHex(cryptoHashOneC(strNew(type), messagePtr, messageSize));

        RETVAL = newSV(strSize(hash));
        SvPOK_only(RETVAL);
        strcpy((char *)SvPV_nolen(RETVAL), strPtr(hash));
        SvCUR_set(RETVAL, strSize(hash));
    }
    MEM_CONTEXT_XS_TEMP_END();
OUTPUT:
    RETVAL
