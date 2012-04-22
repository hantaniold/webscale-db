#include <stdlib.h>
#include "CUnit/Basic.h"
#include "libchidb/util.h"

#define NVALUES (8)

uint16_t uint16_values[] = {0,1,128,255,256,32767,32768,65535};
uint32_t uint32_values[] = {0,255,256,32767,32768,65535,65536,4294967295};
uint32_t varint32_values[] = {0,255,256,32767,32768,65535,65536,268435455};

void test_getput2byte(void)
{
	uint8_t buf[2];
	
	for(int i=0; i<NVALUES; i++)
	{
		uint16_t val;
		put2byte(buf, uint16_values[i]);
		val = get2byte(buf);
		
		CU_ASSERT_EQUAL(val, uint16_values[i]);
	}
}

void test_getput4byte(void)
{
	uint8_t buf[4];
	
	for(int i=0; i<NVALUES; i++)
	{
		uint32_t val;
		put4byte(buf, uint32_values[i]);
		val = get4byte(buf);
		
		CU_ASSERT_EQUAL(val, uint32_values[i]);
	}
}

void test_varint32(void)
{
	uint8_t buf[4];
	
	for(int i=0; i<NVALUES; i++)
	{
		uint32_t val;
		putVarint32(buf, varint32_values[i]);
		getVarint32(buf, &val);
		
		CU_ASSERT_EQUAL(val, varint32_values[i]);
	}
}

int init_tests_utils()
{
	CU_pSuite utilsTests = NULL;
	
	/* add a suite to the registry */
	utilsTests = CU_add_suite("utils", NULL, NULL);
	if (NULL == utilsTests) 
	{
		CU_cleanup_registry();
		return CU_get_error();
	}
	
	if (
		(NULL == CU_add_test(utilsTests, "Get/put uint16", test_getput2byte)) ||
		(NULL == CU_add_test(utilsTests, "Get/put uint32", test_getput4byte)) ||
		(NULL == CU_add_test(utilsTests, "Get/put varint32", test_varint32))
	   )
   	{
      CU_cleanup_registry();
      return CU_get_error();
   }
	
	
	return CU_get_error();
}
