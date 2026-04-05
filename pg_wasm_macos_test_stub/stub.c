/* Auto-targeted list: symbols that are undefined in the pg_wasm test binary and exported by
 * the matching `postgres` executable (pg13 pgrx install). Regenerate if pgrx/PG version
 * changes and dyld reports a new missing symbol. */

#include <stdint.h>

/* --- data (mach-o type S in postgres) --- */
void *CacheMemoryContext;
void *CurTransactionContext;
void *CurrentMemoryContext;
void *ErrorContext;
void *MessageContext;
void *PG_exception_stack;
void *PortalContext;
void *PostmasterContext;
void *SPI_tuptable;
uint64_t SPI_processed;
void *TopMemoryContext;
void *TopTransactionContext;
void *error_context_stack;

/* --- functions (mach-o type T in postgres) --- */
void AllocSetContextCreateInternal(void) {}
void CopyErrorData(void) {}
void FreeErrorData(void) {}
void GetCurrentTransactionId(void) {}
void GetCurrentTransactionIdIfAny(void) {}
void GetDatabaseEncoding(void) {}
void IsBinaryCoercible(void) {}
void MemoryContextDelete(void) {}
void MemoryContextGetParent(void) {}
void SPI_connect(void) {}
void SPI_execute(void) {}
void SPI_execute_with_args(void) {}
void SPI_finish(void) {}
void SPI_getbinval(void) {}
void SPI_gettypeid(void) {}
void format_type_extended(void) {}
void pfree(void) {}
void pg_detoast_datum_packed(void) {}
