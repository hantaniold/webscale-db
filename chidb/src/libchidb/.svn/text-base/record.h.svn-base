#ifndef RECORD_H_
#define RECORD_H_

#include <chidbInt.h>

struct DBRecord 
{
	uint8_t *data;
	uint32_t data_len;
	uint8_t nfields;
	uint32_t packed_len;
	uint32_t *types;
	uint32_t *offsets;
};
typedef struct DBRecord DBRecord;

struct DBRecordBuffer 
{
	DBRecord *dbr;
	uint16_t buf_size;
    uint8_t field;
    uint32_t offset;
    uint8_t header_size;	
};
typedef struct DBRecordBuffer DBRecordBuffer;

int chidb_DBRecord_create(DBRecord **dbr, const char *, ...);

int chidb_DBRecord_create_empty(DBRecordBuffer *dbrb, uint8_t nfields);
int chidb_DBRecord_appendInt8(DBRecordBuffer *dbrb, int8_t v);
int chidb_DBRecord_appendInt16(DBRecordBuffer *dbrb, int16_t v);
int chidb_DBRecord_appendInt32(DBRecordBuffer *dbrb, int32_t v);
int chidb_DBRecord_appendNull(DBRecordBuffer *dbrb);
int chidb_DBRecord_appendString(DBRecordBuffer *dbrb,  char *v);
int chidb_DBRecord_finalize(DBRecordBuffer *dbrb, DBRecord **dbr);

int chidb_DBRecord_unpack(DBRecord **dbr, uint8_t *);
int chidb_DBRecord_pack(DBRecord *dbr, uint8_t **);

int chidb_DBRecord_getType(DBRecord *dbr, uint8_t field);

int chidb_DBRecord_getInt8(DBRecord *dbr, uint8_t field, int8_t *v);
int chidb_DBRecord_getInt16(DBRecord *dbr, uint8_t field, int16_t *v);
int chidb_DBRecord_getInt32(DBRecord *dbr, uint8_t field, int32_t *v);
int chidb_DBRecord_getString(DBRecord *dbr, uint8_t field, char **v);
int chidb_DBRecord_getStringLength(DBRecord *dbr, uint8_t field, int *len);

int chidb_DBRecord_print(DBRecord *dbr);


int chidb_DBRecord_destroy(DBRecord *dbr);


#endif /*RECORD_H_*/
