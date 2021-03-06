/***********************************************************************************************************************************
PostgreSQL 9.6 Interface
***********************************************************************************************************************************/
#ifndef POSTGRES_INTERFACE_INTERFACE096_H
#define POSTGRES_INTERFACE_INTERFACE096_H

#include "postgres/interface.h"

/***********************************************************************************************************************************
Functions
***********************************************************************************************************************************/
bool pgInterfaceIs096(const Buffer *controlFile);
PgControl pgInterfaceControl096(const Buffer *controlFile);

/***********************************************************************************************************************************
Test Functions
***********************************************************************************************************************************/
#ifdef DEBUG
    void pgInterfaceControlTest096(PgControl pgControl, Buffer *buffer);
#endif

#endif
