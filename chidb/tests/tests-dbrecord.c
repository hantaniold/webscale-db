#include <stdlib.h>
#include "CUnit/Basic.h"
#include "libchidb/record.h"

#define NVALUES (8)

char *str_values[] = {"foo", "bar", "foobar", "", "scrumptrulescent", "cromulent", "J.Random Hacker", "aaaaaaaaaabbbbbbbbbbaaaaaaaaaabbbbbbbbbbaaaaaaaaaabbbbbbbbbb"};
int8_t int8_values[] = {0,1,32,-32,64,-64,127,-128};
int16_t int16_values[] = {0,1,1000,-1000,20000,-20000,32767,-32768};
int32_t int32_values[] = {0,1,100000,-100000,2147483647,-2147483648};

void test_string(void)
{
	for(int i=0; i<NVALUES; i++)
	{
		DBRecord *dbr;
		char *val; int len;	
		chidb_DBRecord_create(&dbr, "|s|", str_values[i]);
		CU_ASSERT(dbr->nfields == 1);
		chidb_DBRecord_getString(dbr, 0, &val);
		CU_ASSERT_EQUAL(chidb_DBRecord_getType(dbr, 0), SQL_TEXT);
		CU_ASSERT_STRING_EQUAL(str_values[i], val);
		chidb_DBRecord_getStringLength(dbr, 0, &len);
		CU_ASSERT_EQUAL(strlen(str_values[i]), len);		
		chidb_DBRecord_destroy(dbr);
	}
}

void test_int8(void)
{
	for(int i=0; i<NVALUES; i++)
	{
		DBRecord *dbr;
		int8_t val;	
		chidb_DBRecord_create(&dbr, "|i1|", int8_values[i]);
		CU_ASSERT(dbr->nfields == 1);
		CU_ASSERT_EQUAL(chidb_DBRecord_getType(dbr, 0), SQL_INTEGER_1BYTE);
		chidb_DBRecord_getInt8(dbr, 0, &val);
		CU_ASSERT_EQUAL(int8_values[i], val);		
		chidb_DBRecord_destroy(dbr);
	}
}

void test_int16(void)
{
	for(int i=0; i<NVALUES; i++)
	{
		DBRecord *dbr;
		int16_t val;	
		chidb_DBRecord_create(&dbr, "|i2|", int16_values[i]);
		CU_ASSERT(dbr->nfields == 1);
		CU_ASSERT_EQUAL(chidb_DBRecord_getType(dbr, 0), SQL_INTEGER_2BYTE);
		chidb_DBRecord_getInt16(dbr, 0, &val);
		CU_ASSERT_EQUAL(int16_values[i], val);		
		chidb_DBRecord_destroy(dbr);
	}
}

void test_int32(void)
{
	for(int i=0; i<NVALUES; i++)
	{
		DBRecord *dbr;
		int32_t val;	
		chidb_DBRecord_create(&dbr, "|i4|", int32_values[i]);
		CU_ASSERT(dbr->nfields == 1);
		CU_ASSERT_EQUAL(chidb_DBRecord_getType(dbr, 0), SQL_INTEGER_4BYTE);
		chidb_DBRecord_getInt32(dbr, 0, &val);
		CU_ASSERT_EQUAL(int32_values[i], val);		
		chidb_DBRecord_destroy(dbr);
	}
}

void test_null(void)
{
	DBRecord *dbr;
	
	chidb_DBRecord_create(&dbr, "|0|");
	CU_ASSERT(dbr->nfields == 1);
	CU_ASSERT_EQUAL(chidb_DBRecord_getType(dbr, 0), SQL_NULL);
	chidb_DBRecord_destroy(dbr);
}

void test_multiplefields(void)
{
	DBRecord *dbr;
	char *s; int8_t i8; int16_t i16; int32_t i32;
	int len;
	
	for(int i=0; i<NVALUES; i++)
	{
		chidb_DBRecord_create(&dbr, "|s|0|i1|i2|i4|", str_values[i], int8_values[i], int16_values[i], int32_values[i]);
		CU_ASSERT(dbr->nfields == 5);
	
		CU_ASSERT_EQUAL(chidb_DBRecord_getType(dbr, 0), SQL_TEXT);
		chidb_DBRecord_getString(dbr, 0, &s);
		CU_ASSERT_STRING_EQUAL(str_values[i], s);
		chidb_DBRecord_getStringLength(dbr, 0, &len);
		CU_ASSERT_EQUAL(strlen(str_values[i]), len);	
	
		CU_ASSERT_EQUAL(chidb_DBRecord_getType(dbr, 1), SQL_NULL);
		
		CU_ASSERT_EQUAL(chidb_DBRecord_getType(dbr, 2), SQL_INTEGER_1BYTE);
		chidb_DBRecord_getInt8(dbr, 2, &i8);
		CU_ASSERT_EQUAL(int8_values[i], i8);
		
		CU_ASSERT_EQUAL(chidb_DBRecord_getType(dbr, 3), SQL_INTEGER_2BYTE);
		chidb_DBRecord_getInt16(dbr, 3, &i16);
		CU_ASSERT_EQUAL(int16_values[i], i16);
			
		CU_ASSERT_EQUAL(chidb_DBRecord_getType(dbr, 4), SQL_INTEGER_4BYTE);
		chidb_DBRecord_getInt32(dbr, 4, &i32);
		CU_ASSERT_EQUAL(int32_values[i], i32);	
	
		chidb_DBRecord_destroy(dbr);
	}
}

void test_packunpack(void)
{
	DBRecord *dbr1, *dbr2;
	char *s; int8_t i8; int16_t i16; int32_t i32;
	uint8_t *buf;
	int len;
	
	for(int i=0; i<NVALUES; i++)
	{
		chidb_DBRecord_create(&dbr1, "|s|0|i1|i2|i4|", str_values[i], int8_values[i], int16_values[i], int32_values[i]);
		chidb_DBRecord_pack(dbr1, &buf);

		chidb_DBRecord_unpack(&dbr2, buf);
			
		CU_ASSERT_EQUAL(chidb_DBRecord_getType(dbr2, 0), SQL_TEXT);
		chidb_DBRecord_getString(dbr2, 0, &s);
		CU_ASSERT_STRING_EQUAL(str_values[i], s);
		chidb_DBRecord_getStringLength(dbr2, 0, &len);
		CU_ASSERT_EQUAL(strlen(str_values[i]), len);	
	
		CU_ASSERT_EQUAL(chidb_DBRecord_getType(dbr2, 1), SQL_NULL);
		
		CU_ASSERT_EQUAL(chidb_DBRecord_getType(dbr2, 2), SQL_INTEGER_1BYTE);
		chidb_DBRecord_getInt8(dbr2, 2, &i8);
		CU_ASSERT_EQUAL(int8_values[i], i8);
		
		CU_ASSERT_EQUAL(chidb_DBRecord_getType(dbr2, 3), SQL_INTEGER_2BYTE);
		chidb_DBRecord_getInt16(dbr2, 3, &i16);
		CU_ASSERT_EQUAL(int16_values[i], i16);
			
		CU_ASSERT_EQUAL(chidb_DBRecord_getType(dbr2, 4), SQL_INTEGER_4BYTE);
		chidb_DBRecord_getInt32(dbr2, 4, &i32);
		CU_ASSERT_EQUAL(int32_values[i], i32);	
	
		chidb_DBRecord_destroy(dbr1);
		chidb_DBRecord_destroy(dbr2);
		free(buf);
	}
}

int init_tests_dbrecord()
{
	CU_pSuite dbrecordTests = NULL;
	
	/* add a suite to the registry */
	dbrecordTests = CU_add_suite("dbrecord", NULL, NULL);
	if (NULL == dbrecordTests) 
	{
		CU_cleanup_registry();
		return CU_get_error();
	}
	
	if (
		(NULL == CU_add_test(dbrecordTests, "Single-string record", test_string)) ||
		(NULL == CU_add_test(dbrecordTests, "Single-int8 record", test_int8)) ||
		(NULL == CU_add_test(dbrecordTests, "Single-int16 record", test_int16)) ||
		(NULL == CU_add_test(dbrecordTests, "Single-int32 record", test_int32)) ||
		(NULL == CU_add_test(dbrecordTests, "Single-null record", test_null))||
		(NULL == CU_add_test(dbrecordTests, "Multiple-field record", test_multiplefields))||
		(NULL == CU_add_test(dbrecordTests, "Packing/unpacking a record", test_packunpack))
	   )
   	{
      CU_cleanup_registry();
      return CU_get_error();
   }
	
	
	return CU_get_error();
}
