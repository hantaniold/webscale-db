#include <stdlib.h>
#include "CUnit/Basic.h"
#include "libchidb/btree.h"
#include "libchidb/util.h"
#include "libchidb/dbm.h"
#include "libchidb/record.h"

#define TESTFILE_1 ("test1.cdb") // String database w/ five pages, single B-Tree
#define TESTFILE_2 ("test2.cdb") // Corrupt header
#define TESTFILE_3 ("test3.cdb") // String database w/ seven pages, two B-Trees in 1, 5
#define TESTFILE_4 ("test4.cdb") // Corrupt header, in devious ways
#define TESTFILE_5 ("test5.cdb") // Corrupt header, in even more devious ways
#define TESTFILE_6 ("test6.cdb") // Record database, no indexes

#define NEWFILE ("new.cdb") // Used for new databases

#define TEMPFILE ("temp.cdb") // Used to modify temporary files

#define MULTIINDEXFILE ("tableindex_multipage.cdb") 

key_t file1_keys[] = {1,2,3,7,10,15,20,35,37,42,127,1000,2000,3000,4000,5000};
char *file1_values[] = {"foo1","foo2","foo3","foo7","foo10","foo15",
			"foo20","foo35","foo37","foo42","foo127","foo1000",
			"foo2000","foo3000","foo4000","foo5000"};
key_t file1_nvalues = 16;

#include "bigfile.c"

FILE *copy(const char *from, const char *to)
{
  FILE *fromf, *tof;
  char ch;
  if ((fromf = fopen(from, "rb")) == NULL || (tof = fopen(to, "wb")) == NULL) 
    return NULL;
  /* copy the file */
  while (!feof(fromf)) {
    ch = fgetc(fromf);
    if (ferror(fromf))
      return NULL;
    fputc(ch, tof);
    if (ferror(tof))
      return NULL;
  }
  if (fclose(fromf)==EOF || fclose(tof)==EOF)
    return NULL;		
  return tof;
}

void create_temp_file(const char *from)
{
  remove(TEMPFILE);
  if(copy(from, TEMPFILE) == NULL)
    CU_FAIL_FATAL("Could not create temp file.");
}

void btn_sanity_check(BTree *bt, BTreeNode *btn, bool empty)
{
  CU_ASSERT_FATAL(btn->page->npage >= 1);	
  int header_offset = btn->page->npage==1? 100:0;
  CU_ASSERT_FATAL(btn->type == PGTYPE_TABLE_INTERNAL || 
		  btn->type == PGTYPE_TABLE_LEAF || 
		  btn->type == PGTYPE_INDEX_INTERNAL || 
		  btn->type == PGTYPE_INDEX_LEAF);
  CU_ASSERT_FATAL(btn->n_cells >= 0);
  switch (btn->type) {
  case PGTYPE_TABLE_INTERNAL: 
  case PGTYPE_INDEX_INTERNAL:
    CU_ASSERT_FATAL(btn->free_offset == header_offset + 
		    INTPG_CELLSOFFSET_OFFSET + 
		    (btn->n_cells * 2));
    CU_ASSERT_FATAL(btn->celloffset_array == btn->page->data + 
		    header_offset + 
		    INTPG_CELLSOFFSET_OFFSET);
    break;
  case PGTYPE_TABLE_LEAF: 
  case PGTYPE_INDEX_LEAF:
    CU_ASSERT_FATAL(btn->free_offset == header_offset + 
		    LEAFPG_CELLSOFFSET_OFFSET + 
		    (btn->n_cells * 2));
    CU_ASSERT_FATAL(btn->celloffset_array == btn->page->data + 
		    header_offset + 
		    LEAFPG_CELLSOFFSET_OFFSET);
    break;
  }
  CU_ASSERT_FATAL(btn->cells_offset >= btn->free_offset);
  CU_ASSERT_FATAL(btn->cells_offset <= bt->pager->page_size);
  for (int i=0; i<btn->n_cells; i++) {
    uint16_t cell_offset = get2byte(&btn->celloffset_array[i*2]);
    CU_ASSERT_FATAL(cell_offset >= btn->cells_offset);
    CU_ASSERT_FATAL(cell_offset <= bt->pager->page_size);
  }	
  if (!empty && 
      (btn->type == PGTYPE_TABLE_INTERNAL || btn->type == PGTYPE_INDEX_INTERNAL)) {
    CU_ASSERT_FATAL(btn->right_page > 1);
    CU_ASSERT_FATAL(btn->right_page <= bt->pager->n_pages);
  }
}

void btnNew_sanity_check(BTree *bt, BTreeNode *btn, uint8_t type)
{
  bool leaf = (type == PGTYPE_TABLE_LEAF || type == PGTYPE_INDEX_LEAF);
  btn_sanity_check(bt, btn, true);
  CU_ASSERT_FATAL(btn->type == type);
  CU_ASSERT_FATAL(btn->n_cells == 0);
  CU_ASSERT_FATAL(btn->free_offset == 
		  (leaf? LEAFPG_CELLSOFFSET_OFFSET : INTPG_CELLSOFFSET_OFFSET));
  CU_ASSERT_FATAL(btn->cells_offset == bt->pager->page_size);
  CU_ASSERT_FATAL(btn->celloffset_array == 
		  (uint8_t*) (btn->page->data + 
			      (leaf? LEAFPG_CELLSOFFSET_OFFSET : INTPG_CELLSOFFSET_OFFSET)));
}

void bt_sanity_check(BTree *bt, npage_t nroot)
{
  return;
}


/********************************
 * 
 * Step 1a: Opening an existing chidb file
 * 
 ********************************/

void test_1a_1(void)
{
  int rc;
  chidb *db;
  
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(TESTFILE_1, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  CU_ASSERT(db->bt->pager->n_pages == 5);
  CU_ASSERT(db->bt->pager->page_size == 1024);
  rc = chidb_Btree_close(db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  free(db);
}

void test_1a_2(void)
{
  int rc;
  chidb *db;
  
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(TESTFILE_2, db, &db->bt);
  CU_ASSERT(rc == CHIDB_ECORRUPTHEADER);
  rc = chidb_Btree_open(TESTFILE_4, db, &db->bt);
  CU_ASSERT(rc == CHIDB_ECORRUPTHEADER);
  rc = chidb_Btree_open(TESTFILE_5, db, &db->bt);
  CU_ASSERT(rc == CHIDB_ECORRUPTHEADER);
  
  free(db);
}

void test_1a_3(void)
{
  int rc;
  chidb *db;
  
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(TESTFILE_3, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  CU_ASSERT(db->bt->pager->n_pages == 7);
  CU_ASSERT(db->bt->pager->page_size == 1024);
  rc = chidb_Btree_close(db->bt);
  free(db);
}



/**********************************************
 * 
 * Step 2: Loading a B-Tree node from the file
 * 
 **********************************************/


void test_2_1(void)
{
  int rc;
  chidb *db;
  BTreeNode *btn;
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TESTFILE_1, db, &db->bt);
  
  rc = chidb_Btree_getNodeByPage(db->bt, 1, &btn);
  CU_ASSERT(rc == CHIDB_OK);
  
  rc = chidb_Btree_freeMemNode(db->bt, btn);
  CU_ASSERT(rc == CHIDB_OK);
  
  chidb_Btree_close(db->bt);
  free(db);
}


void test_2_2(void)
{
  chidb *db;
  BTreeNode *btn;
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TESTFILE_1, db, &db->bt);
  chidb_Btree_getNodeByPage(db->bt, 1, &btn);
  
  btn_sanity_check(db->bt, btn, false);
  CU_ASSERT(btn->page->npage == 1);	
  CU_ASSERT(btn->type == PGTYPE_TABLE_INTERNAL);
  CU_ASSERT(btn->n_cells == 3);
  CU_ASSERT(btn->right_page == 2);
  CU_ASSERT(btn->free_offset == 118);
  CU_ASSERT(btn->cells_offset == 1000);
  CU_ASSERT(get2byte(&btn->celloffset_array[0]) == 1008);
  CU_ASSERT(get2byte(&btn->celloffset_array[2]) == 1016);
  CU_ASSERT(get2byte(&btn->celloffset_array[4]) == 1000);
    
  chidb_Btree_freeMemNode(db->bt, btn);
  chidb_Btree_close(db->bt);
  free(db);
}

void test_2_3(void)
{
  chidb *db;
  BTreeNode *btn;
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TESTFILE_1, db, &db->bt);
  chidb_Btree_getNodeByPage(db->bt, 2, &btn);
  
  btn_sanity_check(db->bt, btn, false);
  CU_ASSERT(btn->page->npage == 2);	
  CU_ASSERT(btn->type == PGTYPE_TABLE_LEAF);
  CU_ASSERT(btn->n_cells == 4);
  CU_ASSERT(btn->free_offset == 16);
  CU_ASSERT(btn->cells_offset == 480);
  CU_ASSERT(get2byte(&btn->celloffset_array[0]) == 888);
  CU_ASSERT(get2byte(&btn->celloffset_array[2]) == 752);
  CU_ASSERT(get2byte(&btn->celloffset_array[4]) == 616);
  CU_ASSERT(get2byte(&btn->celloffset_array[6]) == 480);
    
  chidb_Btree_freeMemNode(db->bt, btn);
  chidb_Btree_close(db->bt);
  free(db);
}

void test_2_4(void)
{
  int rc;
  chidb *db;
  BTreeNode *btn;
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TESTFILE_1, db, &db->bt);
  rc = chidb_Btree_getNodeByPage(db->bt, 6, &btn);
  CU_ASSERT(rc == CHIDB_EPAGENO);	
  
  chidb_Btree_close(db->bt);
  free(db);
}


void test_2_5(void)
{
  chidb *db;
  BTreeNode *btn;
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TESTFILE_1, db, &db->bt);
  
  for (int i=1; i<=5; i++) {
    chidb_Btree_getNodeByPage(db->bt, 2, &btn);
    btn_sanity_check(db->bt, btn, false);
    chidb_Btree_freeMemNode(db->bt, btn);
  }
  
  chidb_Btree_close(db->bt);
  free(db);
}


void test_2_6(void)
{
  chidb *db;
  BTreeNode *btn;
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TESTFILE_3, db, &db->bt);
  
  for (int i=1; i<=7; i++) {
    chidb_Btree_getNodeByPage(db->bt, 2, &btn);
    btn_sanity_check(db->bt, btn, false);
    chidb_Btree_freeMemNode(db->bt, btn);
  }

  chidb_Btree_close(db->bt);
  free(db);
}


/****************************************************
 * 
 * Step 3: Creating and writing a B-Tree node to disk
 * 
 ****************************************************/

void test_3_1(void) {
  chidb *db;
  BTreeNode *btn;
  
  create_temp_file(TESTFILE_1);
	
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TEMPFILE, db, &db->bt);
  
  chidb_Btree_getNodeByPage(db->bt, 1, &btn);
  btn->type = PGTYPE_INDEX_INTERNAL;
  btn->n_cells = 2;
  btn->right_page = 3;
  btn->free_offset = 116;
  btn->cells_offset = 1008;
  chidb_Btree_writeNode(db->bt, btn);
  chidb_Btree_freeMemNode(db->bt, btn);	
  
  chidb_Btree_getNodeByPage(db->bt, 1, &btn);
  btn_sanity_check(db->bt, btn, false);
  CU_ASSERT(btn->type == PGTYPE_INDEX_INTERNAL);
  CU_ASSERT(btn->n_cells == 2);
  CU_ASSERT(btn->right_page == 3);
  CU_ASSERT(btn->free_offset == 116);
  CU_ASSERT(btn->cells_offset == 1008);
  chidb_Btree_freeMemNode(db->bt, btn);	
  
  chidb_Btree_close(db->bt);
  free(db);
}
 
void test_3_2(void)
{
  chidb *db;
  BTreeNode *btn;
  
  create_temp_file(TESTFILE_1);
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TEMPFILE, db, &db->bt);
  
  chidb_Btree_getNodeByPage(db->bt, 2, &btn);
  btn->type = PGTYPE_INDEX_LEAF;
  btn->n_cells = 3;
  btn->free_offset = 14;
  btn->cells_offset = 616;
  chidb_Btree_writeNode(db->bt, btn);
  chidb_Btree_freeMemNode(db->bt, btn);	
  
  chidb_Btree_getNodeByPage(db->bt, 2, &btn);
  btn_sanity_check(db->bt, btn, false);
  CU_ASSERT(btn->type == PGTYPE_INDEX_LEAF);
  CU_ASSERT(btn->n_cells == 3);
  CU_ASSERT(btn->free_offset == 14);
  CU_ASSERT(btn->cells_offset == 616);
  chidb_Btree_freeMemNode(db->bt, btn);	
  
  chidb_Btree_close(db->bt);
  free(db);
}


void test_init_empty(BTree *bt, uint8_t type)
{
  BTreeNode *btn;
  npage_t npage;
  
  chidb_Pager_allocatePage(bt->pager, &npage);
  chidb_Btree_initEmptyNode(bt, npage, type);
  
  chidb_Btree_getNodeByPage(bt, npage, &btn);
  btnNew_sanity_check(bt, btn, type);
  chidb_Btree_freeMemNode(bt, btn);	
}

void test_new_node(BTree *bt, uint8_t type)
{
  BTreeNode *btn;
  npage_t npage;
  
  chidb_Btree_newNode(bt, &npage, type);
  chidb_Btree_getNodeByPage(bt, npage, &btn);
  btnNew_sanity_check(bt, btn, type);
  chidb_Btree_freeMemNode(bt, btn);	
}


void test_3_3(void)
{
  chidb *db;
  
  create_temp_file(TESTFILE_1);
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TEMPFILE, db, &db->bt);
  test_init_empty(db->bt, PGTYPE_TABLE_INTERNAL);
  chidb_Btree_close(db->bt);
  free(db);
}

void test_3_4(void)
{
  chidb *db;
  
  create_temp_file(TESTFILE_1);
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TEMPFILE, db, &db->bt);
  test_init_empty(db->bt, PGTYPE_TABLE_LEAF);
  chidb_Btree_close(db->bt);
  free(db);
}

void test_3_5(void)
{
  chidb *db;
  
  create_temp_file(TESTFILE_1);
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TEMPFILE, db, &db->bt);
  test_init_empty(db->bt, PGTYPE_INDEX_INTERNAL);
  chidb_Btree_close(db->bt);
  free(db);
}

void test_3_6(void)
{
  chidb *db;
  
  create_temp_file(TESTFILE_1);
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TEMPFILE, db, &db->bt);
  test_init_empty(db->bt, PGTYPE_INDEX_LEAF);
  chidb_Btree_close(db->bt);
  free(db);
}

void test_3_7(void)
{
  chidb *db;
  
  create_temp_file(TESTFILE_1);
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TEMPFILE, db, &db->bt);
  test_new_node(db->bt, PGTYPE_TABLE_INTERNAL);
  chidb_Btree_close(db->bt);
  free(db);
}

void test_3_8(void)
{
  chidb *db;
  
  create_temp_file(TESTFILE_1);
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TEMPFILE, db, &db->bt);
  test_new_node(db->bt, PGTYPE_TABLE_LEAF);
  chidb_Btree_close(db->bt);
  free(db);
}

void test_3_9(void)
{
  chidb *db;
  
  create_temp_file(TESTFILE_1);
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TEMPFILE, db, &db->bt);
  test_new_node(db->bt, PGTYPE_INDEX_INTERNAL);
  chidb_Btree_close(db->bt);
  free(db);
}

void test_3_10(void)
{
  chidb *db;
  
  create_temp_file(TESTFILE_1);
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TEMPFILE, db, &db->bt);
  test_new_node(db->bt, PGTYPE_INDEX_LEAF);
  chidb_Btree_close(db->bt);
  free(db);
}




/*********************************************
 * 
 * Step 1b: Opening a new chidb file
 * 
 ********************************************/

void test_1b_1(void)
{
  int rc;
  chidb *db;
  
  remove(NEWFILE);
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(NEWFILE, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  CU_ASSERT(db->bt->pager->n_pages == 1);
  CU_ASSERT(db->bt->pager->page_size == 1024);
  
  rc = chidb_Btree_close(db->bt);
  free(db);
}

void test_1b_2(void)
{
  int rc;
  chidb *db;
  MemPage *page;
  uint8_t *rawpage;
  
  remove(NEWFILE);
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(NEWFILE, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  
  rc = chidb_Pager_readPage(db->bt->pager, 1, &page);
  CU_ASSERT(rc == CHIDB_OK);
  rawpage = page->data;
  
  if (strcmp((char *) rawpage, "SQLite format 3") || 
      rawpage[18] != 1 || 
      rawpage[19] != 1 || 
      rawpage[20] != 0 || 
      rawpage[21] != 64 || 
      rawpage[22] != 32 || 
      rawpage[23] != 32 ||
      get4byte(&rawpage[32]) != 0 || 
      get4byte(&rawpage[36]) != 0 || 
      get4byte(&rawpage[44]) != 1 || 
      get4byte(&rawpage[52]) != 0 || 
      get4byte(&rawpage[56]) != 1 || 
      get4byte(&rawpage[64]) != 0 ||
      get4byte(&rawpage[48]) != 20000)
    CU_FAIL("File header is not well-formed.");	
  
  if (rawpage[100] != PGTYPE_TABLE_LEAF || 
      get2byte(&rawpage[101]) != 108 || 
      get2byte(&rawpage[103]) != 0 ||
      get2byte(&rawpage[105]) != 1024 || 
      rawpage[107] != 0)
    CU_FAIL("Page 1 header is not well-formed.");	
  
  rc = chidb_Btree_close(db->bt);
  remove(NEWFILE);
  free(db);
}

void test_1b_3(void)
{
  int rc;
  chidb *db;
  
  remove(NEWFILE);
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(NEWFILE, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  
  rc = chidb_Btree_close(db->bt);
  remove(NEWFILE);
  free(db);
}


/*********************************************
 * 
 * Step 4: Manipulating B-Tree cells
 * 
 ********************************************/

void test_4_1(void)
{
  chidb *db;
  BTreeNode *btn;
  BTreeCell btc;
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TESTFILE_1, db, &db->bt);
  chidb_Btree_getNodeByPage(db->bt, 1, &btn);
  
  chidb_Btree_getCell(btn, 1, &btc);
  CU_ASSERT(btc.type == PGTYPE_TABLE_INTERNAL);
  CU_ASSERT(btc.key == 35);
  CU_ASSERT(btc.fields.tableInternal.child_page == 3);
  
  chidb_Btree_close(db->bt);
  free(db);
}

void test_4_2(void)
{
  chidb *db;
  BTreeNode *btn;
  BTreeCell btc;
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TESTFILE_1, db, &db->bt);
  chidb_Btree_getNodeByPage(db->bt, 5, &btn);
  
  chidb_Btree_getCell(btn, 2, &btc);
  CU_ASSERT(btc.type == PGTYPE_TABLE_LEAF);
  CU_ASSERT(btc.key == 127);
  CU_ASSERT(btc.fields.tableLeaf.data_size == 128);	
  CU_ASSERT(!strcmp(btc.fields.tableLeaf.data, "foo127"));
  
  chidb_Btree_close(db->bt);
  free(db);
}

void test_4_3(void)
{
  chidb *db;
  BTreeNode *btn;
  BTreeCell btc;
  ncell_t old_ncell;
  uint16_t old_freeoffset;
  
  for (ncell_t cellpos = 0; cellpos < 4; cellpos++) {
    create_temp_file(TESTFILE_1);
    
    db = malloc(sizeof(chidb));
    chidb_Btree_open(TEMPFILE, db, &db->bt);
    
    chidb_Btree_getNodeByPage(db->bt, 1, &btn);
    old_ncell = btn->n_cells;
    old_freeoffset = btn->free_offset;
    btc.key = 500;
    btc.type = PGTYPE_TABLE_INTERNAL;
    btc.fields.tableInternal.child_page = 6;
    chidb_Btree_insertCell(btn, cellpos, &btc);
    btn_sanity_check(db->bt, btn, false);
    CU_ASSERT(btn->n_cells == old_ncell + 1);
    CU_ASSERT(btn->free_offset == old_freeoffset + 2);
    
    chidb_Btree_writeNode(db->bt, btn);
    
    chidb_Btree_getNodeByPage(db->bt, 1, &btn);
    btn_sanity_check(db->bt, btn, false);
    chidb_Btree_getCell(btn, cellpos, &btc);
    CU_ASSERT(btc.type == PGTYPE_TABLE_INTERNAL);
    CU_ASSERT(btc.key == 500);
    CU_ASSERT(btc.fields.tableInternal.child_page == 6);
    
    chidb_Btree_close(db->bt);
    free(db);
  }
}

void test_4_4(void)
{
  chidb *db;
  BTreeNode *btn;
  BTreeCell btc;
  ncell_t old_ncell;
  uint16_t old_freeoffset;
  char string[128];
  
  for (ncell_t cellpos = 0; cellpos < 4; cellpos++) {	
    create_temp_file(TESTFILE_1);
    
    db = malloc(sizeof(chidb));
    chidb_Btree_open(TEMPFILE, db, &db->bt);
    
    chidb_Btree_getNodeByPage(db->bt, 3, &btn);
    old_ncell = btn->n_cells;
    old_freeoffset = btn->free_offset;
    btc.key = 123;
    btc.type = PGTYPE_TABLE_LEAF;
    btc.fields.tableLeaf.data_size = 128;
    bzero(string, 128); strcpy(string, "foobar");
    btc.fields.tableLeaf.data = string;	
    chidb_Btree_insertCell(btn, cellpos, &btc);
    btn_sanity_check(db->bt, btn, false);
    CU_ASSERT(btn->n_cells == old_ncell + 1);
    CU_ASSERT(btn->free_offset == old_freeoffset + 2);
    
    chidb_Btree_writeNode(db->bt, btn);
    
    chidb_Btree_getNodeByPage(db->bt, 3, &btn);
    btn_sanity_check(db->bt, btn, false);
    chidb_Btree_getCell(btn, cellpos, &btc);
    CU_ASSERT(btc.type == PGTYPE_TABLE_LEAF);
    CU_ASSERT(btc.key == 123);
    CU_ASSERT(btc.fields.tableLeaf.data_size == 128);
    CU_ASSERT(!strcmp(btc.fields.tableLeaf.data, "foobar"));
    
    chidb_Btree_close(db->bt);
    free(db);
  }
}



void test_values(BTree *bt, key_t *keys, char **values, key_t nkeys)
{
  uint16_t size;
  uint8_t *data;
  int rc;	
  
  for (int i = 0; i<nkeys; i++) {
    rc = chidb_Btree_find(bt, 1, keys[i], &data, &size);
    CU_ASSERT(rc == CHIDB_OK);
    CU_ASSERT(size == 128);
    CU_ASSERT(!strcmp(data, values[i]));
  }
}

void test_5_1(void)
{
  chidb *db;
  
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TESTFILE_1, db, &db->bt);
  test_values(db->bt, file1_keys, file1_values, file1_nvalues);
  chidb_Btree_close(db->bt);
  free(db);
}

void test_5_2(void)
{
  chidb *db;
  uint16_t size;
  uint8_t *data;
  key_t nokeys[] = {0,4,6,8,9,11,18,27,36,40,100,650,1500,2500,3500,4500,5500};
  int rc;
	
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TESTFILE_1, db, &db->bt);
  for (int i = 0; i<16; i++) {
    rc = chidb_Btree_find(db->bt, 1, nokeys[i], &data, &size);
    CU_ASSERT(rc == CHIDB_ENOTFOUND);
  }
  chidb_Btree_close(db->bt);
  free(db);
}



void test_6_1(void)
{
  chidb *db;
  int rc;
  npage_t pages[] = {4,3,2};
  key_t keys[] = {4,9,6000};
  char *values[] = {"foo4","foo9","foo6000"};
  
  
  create_temp_file(TESTFILE_1);
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TEMPFILE, db, &db->bt);
  
  for (int i=0; i<3; i++) {
    rc = chidb_Btree_insertInTable(db->bt, pages[i], keys[i], values[i], 128);
    CU_ASSERT(rc == CHIDB_OK);
  }
  
  test_values(db->bt, file1_keys, file1_values, file1_nvalues);
  test_values(db->bt, keys, values, 3);
  
  chidb_Btree_close(db->bt);
  free(db);
}

void test_6_2(void)
{
  chidb *db;
  int rc;
  key_t keys[] = {4,9,6000};
  char *values[] = {"foo4","foo9","foo6000"};
  
  
  create_temp_file(TESTFILE_1);
  db = malloc(sizeof(chidb));
  chidb_Btree_open(TEMPFILE, db, &db->bt);
  
  for (int i=0; i<3; i++) {
    rc = chidb_Btree_insertInTable(db->bt, 1, keys[i], values[i], 128);
    CU_ASSERT(rc == CHIDB_OK);
  }
  
  test_values(db->bt, file1_keys, file1_values, file1_nvalues);
  test_values(db->bt, keys, values, 3);
  
  chidb_Btree_close(db->bt);
  free(db);
}

void insert_bigfile(chidb *db, int i)
{
  int rc;
  int datalen = ((bigfile_pkeys[i] % 3) + 1) * 64;
  uint8_t buf[192];
  
  for (int j=0; j<48; j++)
    put4byte(buf + (4*j), bigfile_ikeys[i]);
  
  rc = chidb_Btree_insertInTable(db->bt, 1, bigfile_pkeys[i], buf, datalen);
  CU_ASSERT(rc == CHIDB_OK);
  
}

void test_bigfile(chidb *db)
{
  int rc;
  
  for (int i=0; i<bigfile_nvalues; i++) {
    uint8_t* buf;
    uint16_t size;
    uint8_t data[192];
    int datalen = ((bigfile_pkeys[i] % 3) + 1) * 64;
    
    for (int j=0; j<48; j++)
      put4byte(data + (4*j), bigfile_ikeys[i]);
    
    rc = chidb_Btree_find(db->bt, 1, bigfile_pkeys[i], &buf, &size);
    if(rc) printf("Error at %i\n", bigfile_pkeys[i]);
    CU_ASSERT(rc == CHIDB_OK);
    CU_ASSERT(size == datalen);
    CU_ASSERT(!memcmp(buf, data, datalen));
    
    //SHOW_ALL_KEYS(db->bt);
  }
  //SHOW_ALL_KEYS(db->bt);
}

int chidb_Btree_findInIndex(BTree *bt, npage_t nroot, key_t ikey, key_t *pkey)
{
  BTreeNode *btn;
  
  chidb_Btree_getNodeByPage(bt, nroot, &btn);
  
  if (btn->type == PGTYPE_INDEX_LEAF) {
    for (int i = 0; i<btn->n_cells; i++) {
      BTreeCell btc;
      chidb_Btree_getCell(btn, i, &btc);
      if (btc.key == ikey) {
	*pkey = btc.fields.indexLeaf.keyPk;
	return CHIDB_OK;
      }
    }
    return CHIDB_ENOTFOUND;		
  } else if (btn->type == PGTYPE_INDEX_INTERNAL) {
    for (int i = 0; i<btn->n_cells; i++) {
      BTreeCell btc;
      chidb_Btree_getCell(btn, i, &btc);      
      if (btc.key == ikey) {
	*pkey = btc.fields.indexInternal.keyPk;
	return CHIDB_OK;
      }
      if (ikey <= btc.key)
	return chidb_Btree_findInIndex(bt, btc.fields.indexInternal.child_page, ikey, pkey);
    }
    return chidb_Btree_findInIndex(bt, btn->right_page, ikey, pkey);
  }
  chidb_Btree_freeMemNode(bt, btn);		
  return CHIDB_OK;
}

void test_index_bigfile(chidb *db, npage_t index_nroot)
{
  int rc;
  for (int i=0; i<bigfile_nvalues; i++) {
    uint8_t* buf;
    uint16_t size;
    uint8_t data[192];
    key_t pkey;
    
    rc = chidb_Btree_findInIndex(db->bt, index_nroot, bigfile_ikeys[i], &pkey);
    CU_ASSERT(rc == CHIDB_OK);
    
    int datalen = ((pkey % 3) + 1) * 64;
    
    for(int j=0; j<48; j++)
      put4byte(data + (4*j), bigfile_ikeys[i]);
    
    rc = chidb_Btree_find(db->bt, 1, pkey, &buf, &size);
    if(rc) printf("Error at key %i\n", bigfile_ikeys[i]);
    CU_ASSERT(rc == CHIDB_OK);
    CU_ASSERT(size == datalen);
    CU_ASSERT(!memcmp(buf, data, datalen));
  }
}

void test_7_1(void)
{
  chidb *db;
  int rc;
  
  remove(NEWFILE);
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(NEWFILE, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  
  for (int i=0; i<bigfile_nvalues; i++)
    insert_bigfile(db, i);	
  
  test_bigfile(db);
  
  chidb_Btree_close(db->bt);
  free(db);
}

void test_7_2(void)
{
  chidb *db;
  int rc;
  
  remove(NEWFILE);
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(NEWFILE, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  
  for (int i=bigfile_nvalues-1; i>=0; i--)
    insert_bigfile(db, i);

  test_bigfile(db);
  
  chidb_Btree_close(db->bt);
  free(db);
}

void test_7_3(void)
{
  chidb *db;
  int rc;
  
  remove(NEWFILE);
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(NEWFILE, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  
  for(int i=0; i<bigfile_nvalues; i+=2)
    insert_bigfile(db, i);	
  for(int i=1; i<bigfile_nvalues; i+=2)
    insert_bigfile(db, i);	
  
  test_bigfile(db);
  
  chidb_Btree_close(db->bt);
  free(db);
}


void test_8_1(void)
{
  chidb *db;
  int rc;
  npage_t npage;
  
  remove(NEWFILE);
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(NEWFILE, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  
  for (int i=0; i<bigfile_nvalues; i++)
    insert_bigfile(db, i);	
  
  chidb_Btree_newNode(db->bt, &npage, PGTYPE_INDEX_LEAF);
  for (int i=0; i<bigfile_nvalues; i++)
    chidb_Btree_insertInIndex(db->bt, npage, bigfile_ikeys[i], bigfile_pkeys[i]);
  
  test_index_bigfile(db, npage);
 
  chidb_Btree_close(db->bt);
  free(db);
}


void test_8_2(void)
{
  chidb *db;
  int rc;
  npage_t npage;
  
  remove(NEWFILE);
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(NEWFILE, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  
  for(int i=0; i<bigfile_nvalues; i++)
    insert_bigfile(db, i);	
  
  chidb_Btree_newNode(db->bt, &npage, PGTYPE_INDEX_LEAF);
  for(int i=bigfile_nvalues-1; i>=0; i--)
    chidb_Btree_insertInIndex(db->bt, npage, bigfile_ikeys[i], bigfile_pkeys[i]);
  
  test_index_bigfile(db, npage);
  
  chidb_Btree_close(db->bt);
  free(db);
}


void test_8_3(void)
{
  chidb *db;
  int rc;
  npage_t npage;
  
  remove(NEWFILE);
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(NEWFILE, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  
  for (int i=0; i<bigfile_nvalues; i++)
    insert_bigfile(db, i);	
  
  chidb_Btree_newNode(db->bt, &npage, PGTYPE_INDEX_LEAF);
  for(int i=0; i<bigfile_nvalues; i+=2)
    chidb_Btree_insertInIndex(db->bt, npage, bigfile_ikeys[i], bigfile_pkeys[i]);
  for(int i=1; i<bigfile_nvalues; i+=2)
    chidb_Btree_insertInIndex(db->bt, npage, bigfile_ikeys[i], bigfile_pkeys[i]);
  
  test_index_bigfile(db, npage);
  
  chidb_Btree_close(db->bt);
  free(db);
}

//STORES A STRING IN THE DBM AT SPECIFIED REGISTER
void string_inst(dbm *input_dbm, uint32_t r_num, const char *str) {
	chidb_instruction inst;
	uint32_t old_pc = input_dbm->program_counter;
	inst.instruction = DBM_STRING;
	if (str != NULL) {
		inst.P1 = (uint32_t)strlen(str);
		inst.P2 = r_num;
		inst.P4 = str;
		CU_ASSERT(tick_dbm(input_dbm, inst) == DBM_OK);
		CU_ASSERT(input_dbm->registers[r_num].type == STRING);
		CU_ASSERT(strncmp(input_dbm->registers[r_num].data.str_val, str, input_dbm->registers[r_num].data_len) == 0);
		CU_ASSERT(input_dbm->program_counter == (old_pc + 1));
	} else {
		inst.P1 = 0;
		inst.P2 = r_num;
		inst.P4 = NULL;
		CU_ASSERT(tick_dbm(input_dbm, inst) == DBM_OK);
		CU_ASSERT(input_dbm->registers[r_num].type == STRING);
		CU_ASSERT(input_dbm->registers[r_num].data.str_val == NULL);
		CU_ASSERT(input_dbm->program_counter == (old_pc + 1));
	}
}

void integer_inst(dbm *input_dbm, uint32_t r_num, int32_t val) {
	chidb_instruction inst;
	inst.instruction = DBM_INTEGER;
	uint32_t old_pc = input_dbm->program_counter;
	inst.P1 = val;
	inst.P2 = r_num;
	CU_ASSERT(tick_dbm(input_dbm, inst) == DBM_OK);
	CU_ASSERT(input_dbm->registers[r_num].type == INTEGER);
	CU_ASSERT(input_dbm->registers[r_num].data.int_val == val);
	CU_ASSERT(input_dbm->program_counter == (old_pc + 1));
}

void null_inst(dbm *input_dbm, uint32_t r_num) {
	chidb_instruction inst;
	inst.instruction = DBM_NULL;
	uint32_t old_pc = input_dbm->program_counter;
	inst.P2 = r_num;
	CU_ASSERT(tick_dbm(input_dbm, inst) == DBM_OK);
	CU_ASSERT(input_dbm->registers[r_num].type == NL);
	CU_ASSERT(input_dbm->program_counter == (old_pc + 1));
}

void eq_inst(dbm *input_dbm, uint32_t r_num1, uint32_t jump_addr, uint32_t r_num2) {
	chidb_instruction inst;
	inst.instruction = DBM_EQ;
	inst.P1 = r_num1;
	inst.P2 = jump_addr;
	inst.P3 = r_num2;
	CU_ASSERT(tick_dbm(input_dbm, inst) == DBM_OK);
}

void lt_inst(dbm *input_dbm, uint32_t r_num1, uint32_t jump_addr, uint32_t r_num2) {
	chidb_instruction inst;
	inst.instruction = DBM_LT;
	inst.P1 = r_num1;
	inst.P2 = jump_addr;
	inst.P3 = r_num2;
	CU_ASSERT(tick_dbm(input_dbm, inst) == DBM_OK);
} 

void le_inst(dbm *input_dbm, uint32_t r_num1, uint32_t jump_addr, uint32_t r_num2) {
	chidb_instruction inst;
	inst.instruction = DBM_LE;
	inst.P1 = r_num1;
	inst.P2 = jump_addr;
	inst.P3 = r_num2;
	CU_ASSERT(tick_dbm(input_dbm, inst) == DBM_OK);
} 

void gt_inst(dbm *input_dbm, uint32_t r_num1, uint32_t jump_addr, uint32_t r_num2) {
	chidb_instruction inst;
	inst.instruction = DBM_GT;
	inst.P1 = r_num1;
	inst.P2 = jump_addr;
	inst.P3 = r_num2;
	CU_ASSERT(tick_dbm(input_dbm, inst) == DBM_OK);
} 

void ge_inst(dbm *input_dbm, uint32_t r_num1, uint32_t jump_addr, uint32_t r_num2) {
	chidb_instruction inst;
	inst.instruction = DBM_GE;
	inst.P1 = r_num1;
	inst.P2 = jump_addr;
	inst.P3 = r_num2;
	CU_ASSERT(tick_dbm(input_dbm, inst) == DBM_OK);
} 

void ne_inst(dbm * input_dbm, uint32_t r_num1, uint32_t jump_addr, uint32_t r_num2) {
    chidb_instruction inst;
    inst.instruction = DBM_NE;
    inst.P1 = r_num1;
	inst.P2 = jump_addr;
	inst.P3 = r_num2;
	CU_ASSERT(tick_dbm(input_dbm, inst) == DBM_OK);
}

void reset_assert(dbm *input_dbm) {
	CU_ASSERT(reset_dbm(input_dbm) == CHIDB_OK);
	CU_ASSERT(input_dbm->program_counter == 0);  
}

void halt_inst(dbm *input_dbm, uint32_t error_code, char *error_message) {
	chidb_instruction inst;
	inst.instruction = DBM_HALT;
	inst.P1 = error_code;
	inst.P4 = error_message;
	CU_ASSERT(tick_dbm(input_dbm, inst) == DBM_HALT_STATE);
	if (error_code == 0) {
		CU_ASSERT(input_dbm->tick_result == DBM_OK);
	} else {
		CU_ASSERT(input_dbm->tick_result == error_code);
		CU_ASSERT(strcmp(input_dbm->error_str, error_message) == 0);
	}
}

void test_9_1(void)
{
	dbm* test_dbm = init_dbm(NULL);
	printf("\nTest DBM_INTEGER...\n");
	integer_inst(test_dbm, 10, 21678);
	reset_assert(test_dbm);
	
	integer_inst(test_dbm, 10, NULL);
	reset_assert(test_dbm);
	
	printf("Test DBM_STRING...\n");
	string_inst(test_dbm, 200, "charles\0");
	CU_ASSERT(strcmp(test_dbm->registers[200].data.str_val, "ch56les\0") != 0);
	CU_ASSERT(strcmp(test_dbm->registers[200].data.str_val, "charles\0") == 0);
	reset_assert(test_dbm);
	
	string_inst(test_dbm, 200, NULL);
	CU_ASSERT(test_dbm->registers[200].data.str_val == NULL);
	
	printf("Test DBM_NULL...\n");
	null_inst(test_dbm, 0);
	reset_assert(test_dbm);
	free(test_dbm);
}

void test_9_2(void) {
	//DBM_EQ
	dbm* test_dbm = init_dbm(NULL);
	string_inst(test_dbm, 100, "charles\0");
	string_inst(test_dbm, 200, "charles\0");
	eq_inst(test_dbm, 100, 7, 200);
	CU_ASSERT(test_dbm->program_counter == 7);
	reset_assert(test_dbm);
	
	string_inst(test_dbm, 100, "charles\0");
	string_inst(test_dbm, 200, "bob\0");
	eq_inst(test_dbm, 100, 7, 200);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	
	string_inst(test_dbm, 100, "charles\0");
	string_inst(test_dbm, 200, "cHarle1\0");
	eq_inst(test_dbm, 100, 7, 200);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	
	integer_inst(test_dbm, 0, 1234567);
	integer_inst(test_dbm, 1, 1234567);
	eq_inst(test_dbm, 0, 77, 1);
	CU_ASSERT(test_dbm->program_counter == 77);
	reset_assert(test_dbm);
	
	integer_inst(test_dbm, 0, 1234567);
	integer_inst(test_dbm, 1, -1);
	eq_inst(test_dbm, 0, 77, 1);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	
	integer_inst(test_dbm, 0, -1);
	integer_inst(test_dbm, 1, -1);
	eq_inst(test_dbm, 0, 77, 1);
	CU_ASSERT(test_dbm->program_counter == 77);
	CU_ASSERT(test_dbm->registers[0].data.int_val == -1);
	
	null_inst(test_dbm, 10);
	null_inst(test_dbm, 100);
	eq_inst(test_dbm, 10, 77, 100);
	CU_ASSERT(test_dbm->program_counter == 77);
	reset_assert(test_dbm);
	
	null_inst(test_dbm, 10);
	integer_inst(test_dbm, 1, NULL);
	eq_inst(test_dbm, 10, 77, 100);
	CU_ASSERT(test_dbm->program_counter == 77);
	reset_assert(test_dbm);
	
	null_inst(test_dbm, 10);
	integer_inst(test_dbm, 100, 4);
	eq_inst(test_dbm, 10, 77, 100);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	
	null_inst(test_dbm, 10);
	string_inst(test_dbm, 100, NULL);
	eq_inst(test_dbm, 10, 77, 100);
	CU_ASSERT(test_dbm->program_counter == 77);
	reset_assert(test_dbm);
	
	null_inst(test_dbm, 10);
	string_inst(test_dbm, 100, "charles\0");
	eq_inst(test_dbm, 10, 77, 100);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	
	free(test_dbm);
}
void test_9_3(void) {
	//DBM_NE
	dbm* test_dbm = init_dbm(NULL);

    integer_inst(test_dbm, 0, 123);
    integer_inst(test_dbm, 1, 44);
    ne_inst(test_dbm, 0, 42, 1);
    CU_ASSERT(test_dbm->program_counter == 42);
    CU_ASSERT(test_dbm->registers[0].data.int_val == 123);
    CU_ASSERT(test_dbm->registers[1].data.int_val == 44);
    reset_assert(test_dbm);

    integer_inst(test_dbm, 21, 4414);
    integer_inst(test_dbm, 55, 4414);
    ne_inst(test_dbm, 21, 44, 55);
    CU_ASSERT(test_dbm->program_counter == 3);
    reset_assert(test_dbm);
 
    string_inst(test_dbm, 0, "banana\0");
    string_inst(test_dbm, 1, "banana\0");
    ne_inst(test_dbm, 0, 42, 1);
    CU_ASSERT(test_dbm->program_counter == 3);
    reset_assert(test_dbm);

    string_inst(test_dbm, 0, "banana\0");
    string_inst(test_dbm, 1, "banerana\0");
    ne_inst(test_dbm, 0, 44, 1); 
    CU_ASSERT(test_dbm->program_counter == 44);
    reset_assert(test_dbm);   

    null_inst(test_dbm, 10);
	null_inst(test_dbm, 100);
	ne_inst(test_dbm, 10, 77, 100);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	
    string_inst(test_dbm,10,"horse\0");
    null_inst(test_dbm, 100);
	ne_inst(test_dbm, 100, 77, 10);
	CU_ASSERT(test_dbm->program_counter == 77);
	reset_assert(test_dbm);
		
    string_inst(test_dbm,10,NULL);
    null_inst(test_dbm, 100);
	ne_inst(test_dbm, 10, 77, 100);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
		
    integer_inst(test_dbm,10,1);
    null_inst(test_dbm, 100);
	ne_inst(test_dbm, 10, 77, 100);
	CU_ASSERT(test_dbm->program_counter == 77);
	reset_assert(test_dbm);
		
    integer_inst(test_dbm,10,NULL);
    null_inst(test_dbm, 100);
	ne_inst(test_dbm, 100, 77, 10);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	

    free(test_dbm);
}

void test_9_4(void) {
	//DBM_LT
	dbm* test_dbm = init_dbm(NULL);
	integer_inst(test_dbm, 0, 123);
	integer_inst(test_dbm, 1, 44);
	lt_inst(test_dbm, 0, 42, 1);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	
	integer_inst(test_dbm, 0, 44);
	integer_inst(test_dbm, 1, 44);
	lt_inst(test_dbm, 0, 42, 1);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	
	integer_inst(test_dbm, 0, 123);
	integer_inst(test_dbm, 1, 1000);
	lt_inst(test_dbm, 0, 42, 1);
	CU_ASSERT(test_dbm->program_counter == 42);
	reset_assert(test_dbm);
	
	string_inst(test_dbm, 100, "charles\0");
	string_inst(test_dbm, 200, "charles\0");
	lt_inst(test_dbm, 100, 42, 200);
	CU_ASSERT(test_dbm->program_counter == 3);
	CU_ASSERT(strcmp(test_dbm->registers[100].data.str_val, "charles\0") == 0);
	reset_assert(test_dbm);
	
	string_inst(test_dbm, 100, "aharles\0");
	string_inst(test_dbm, 200, "charles\0");
	lt_inst(test_dbm, 100, 42, 200);
	CU_ASSERT(test_dbm->program_counter == 42);
	reset_assert(test_dbm);
	
	string_inst(test_dbm, 100, "dharles\0");
	string_inst(test_dbm, 200, "charles\0");
	lt_inst(test_dbm, 100, 42, 200);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	
	free(test_dbm);
}
void test_9_5(void) {
	//DBM_LT
	dbm* test_dbm = init_dbm(NULL);
	integer_inst(test_dbm, 0, 123);
	integer_inst(test_dbm, 1, 44);
	le_inst(test_dbm, 0, 42, 1);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	
	integer_inst(test_dbm, 0, 44);
	integer_inst(test_dbm, 1, 44);
	le_inst(test_dbm, 0, 42, 1);
	CU_ASSERT(test_dbm->program_counter == 42);
	reset_assert(test_dbm);
	
	integer_inst(test_dbm, 0, 123);
	integer_inst(test_dbm, 1, 1000);
	le_inst(test_dbm, 0, 42, 1);
	CU_ASSERT(test_dbm->program_counter == 42);
	reset_assert(test_dbm);
	
	string_inst(test_dbm, 100, "charles\0");
	string_inst(test_dbm, 200, "charles\0");
	le_inst(test_dbm, 100, 42, 200);
	CU_ASSERT(test_dbm->program_counter == 42);
	CU_ASSERT(strcmp(test_dbm->registers[100].data.str_val, "charles\0") == 0);
	reset_assert(test_dbm);
	
	string_inst(test_dbm, 100, "aharles\0");
	string_inst(test_dbm, 200, "charles\0");
	le_inst(test_dbm, 100, 42, 200);
	CU_ASSERT(test_dbm->program_counter == 42);
	reset_assert(test_dbm);
	
	string_inst(test_dbm, 100, "dharles\0");
	string_inst(test_dbm, 200, "charles\0");
	le_inst(test_dbm, 100, 42, 200);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	
	free(test_dbm);
}
void test_9_6(void) {
	//DBM_GT
	dbm* test_dbm = init_dbm(NULL);
	integer_inst(test_dbm, 0, 123);
	integer_inst(test_dbm, 1, 44);
	gt_inst(test_dbm, 0, 42, 1);
	CU_ASSERT(test_dbm->program_counter == 42);
	reset_assert(test_dbm);
	
	integer_inst(test_dbm, 0, 44);
	integer_inst(test_dbm, 1, 44);
	gt_inst(test_dbm, 0, 42, 1);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	
	integer_inst(test_dbm, 0, 123);
	integer_inst(test_dbm, 1, 1000);
	gt_inst(test_dbm, 0, 42, 1);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	
	string_inst(test_dbm, 100, "charles\0");
	string_inst(test_dbm, 200, "charles\0");
	gt_inst(test_dbm, 100, 42, 200);
	CU_ASSERT(test_dbm->program_counter == 3);
	CU_ASSERT(strcmp(test_dbm->registers[100].data.str_val, "charles\0") == 0);
	reset_assert(test_dbm);
	
	string_inst(test_dbm, 100, "aharles\0");
	string_inst(test_dbm, 200, "charles\0");
	gt_inst(test_dbm, 100, 42, 200);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	
	string_inst(test_dbm, 100, "dharles\0");
	string_inst(test_dbm, 200, "charles\0");
	gt_inst(test_dbm, 100, 42, 200);
	CU_ASSERT(test_dbm->program_counter == 42);
	reset_assert(test_dbm);
	
	free(test_dbm);
}
void test_9_7(void) {
	//DBM_GE
	dbm* test_dbm = init_dbm(NULL);
	integer_inst(test_dbm, 0, 123);
	integer_inst(test_dbm, 1, 44);
	ge_inst(test_dbm, 0, 42, 1);
	CU_ASSERT(test_dbm->program_counter == 42);
	reset_assert(test_dbm);
	
	integer_inst(test_dbm, 0, 44);
	integer_inst(test_dbm, 1, 44);
	ge_inst(test_dbm, 0, 42, 1);
	CU_ASSERT(test_dbm->program_counter == 42);
	reset_assert(test_dbm);
	
	integer_inst(test_dbm, 0, 123);
	integer_inst(test_dbm, 1, 1000);
	ge_inst(test_dbm, 0, 42, 1);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	
	string_inst(test_dbm, 100, "charles\0");
	string_inst(test_dbm, 200, "charles\0");
	ge_inst(test_dbm, 100, 42, 200);
	CU_ASSERT(test_dbm->program_counter == 42);
	CU_ASSERT(strcmp(test_dbm->registers[100].data.str_val, "charles\0") == 0);
	reset_assert(test_dbm);
	
	string_inst(test_dbm, 100, "aharles\0");
	string_inst(test_dbm, 200, "charles\0");
	ge_inst(test_dbm, 100, 42, 200);
	CU_ASSERT(test_dbm->program_counter == 3);
	reset_assert(test_dbm);
	
	string_inst(test_dbm, 100, "dharles\0");
	string_inst(test_dbm, 200, "charles\0");
	ge_inst(test_dbm, 100, 42, 200);
	CU_ASSERT(test_dbm->program_counter == 42);
	reset_assert(test_dbm);
	
	free(test_dbm);
}
void test_9_8(void) {
	//DBM_HALT
	dbm* test_dbm = init_dbm(NULL);
	halt_inst(test_dbm, 0, NULL);
	
	reset_assert(test_dbm);
	
	halt_inst(test_dbm, 1, "Sassy error message.");
	free(test_dbm);
}



/* is_error - should the result of this test be an error */
void openread_inst(dbm * input_dbm, uint32_t cursor_nr, uint32_t reg_nr, uint32_t page_nr, uint32_t is_error) {
    chidb_instruction inst;
    int old_pc = input_dbm->program_counter;
    inst.instruction = DBM_OPENREAD;
    inst.P1 = cursor_nr;
    inst.P2 = reg_nr;
    input_dbm->registers[inst.P2].data.int_val = page_nr;
   
    int res = tick_dbm(input_dbm, inst);
    if (!is_error) {
        CU_ASSERT(res == DBM_OK);
        if (res != DBM_OK) return;
        CU_ASSERT(input_dbm->readwritestate == DBM_READ_STATE);
        CU_ASSERT(input_dbm->cursors[cursor_nr].node->page->npage == page_nr);
        CU_ASSERT(old_pc == input_dbm->program_counter + 1);
    } else {
        CU_ASSERT(res == DBM_HALT_STATE);
        CU_ASSERT(old_pc == input_dbm->program_counter);
        CU_ASSERT(input_dbm->tick_result == DBM_OPENRW_ERROR);
    }
    
}

void test_9_9(void) {
	chidb * db;
    int rc;

    remove(MULTIINDEXFILE);
    db = malloc(sizeof(chidb));
    rc = chidb_Btree_open(MULTIINDEXFILE, db, &db->bt);
    CU_ASSERT(rc == CHIDB_OK);
    
    dbm * test_dbm = init_dbm(db);

    openread_inst(test_dbm, 3, 2, -42, 1);
    reset_assert(test_dbm);

    printf("shoop\n");
    openread_inst(test_dbm, 3, 2, 4, 0);
    reset_assert(test_dbm);

    free(test_dbm);
}


void test_9_10(void) {
	chidb * db;
    int rc;

    remove(MULTIINDEXFILE);
    db = malloc(sizeof(chidb));
    rc = chidb_Btree_open(MULTIINDEXFILE, db, &db->bt);
    CU_ASSERT(rc == CHIDB_OK);
    
    dbm * test_dbm = init_dbm(db);

    chidb_instruction inst;
    inst.instruction = DBM_OPENWRITE;



  
	//DBM_OPENWRITE
}

void test_9_11(void) {
	//DBM_CLOSE
}

void test_9_12(void) {
	//DBM_REWIND
	chidb *db;
  db = malloc(sizeof(chidb));
  BTree *bt;
	CU_ASSERT(chidb_Btree_open("singletable_singlepage.cdb", db, &(bt)) == CHIDB_OK);
	dbm* test_dbm = init_dbm(db);
	CU_ASSERT(test_dbm != NULL);
	
	integer_inst(test_dbm, 0, 2);
	
	chidb_instruction inst;
	inst.instruction = DBM_OPENREAD;
	inst.P1 = 0;
	inst.P2 = 0;
	inst.P3 = 4;
	CU_ASSERT(tick_dbm(test_dbm, inst) == DBM_OK);
	
	CU_ASSERT(test_dbm->program_counter == 2);
	inst.instruction = DBM_REWIND;
	inst.P1 = 0;
	inst.P2 = 29;
	
	CU_ASSERT(tick_dbm(test_dbm, inst) == DBM_OK);
	
	CU_ASSERT(test_dbm->program_counter == 3);
	CU_ASSERT(test_dbm->cursors[0].prev_cell == NULL);
	CU_ASSERT(test_dbm->cursors[0].curr_cell != NULL);
	CU_ASSERT(test_dbm->cursors[0].curr_cell->type == PGTYPE_TABLE_LEAF);
	CU_ASSERT(test_dbm->cursors[0].next_cell != NULL);
	
	
	DBRecord *dbr;
	CU_ASSERT(chidb_DBRecord_unpack(&(dbr), test_dbm->cursors[0].curr_cell->fields.tableLeaf.data) == CHIDB_OK);
	
	int8_t *val8 = (int8_t *)malloc(sizeof(int8_t));
	int16_t *val16 = (int16_t *)malloc(sizeof(int16_t));
	int32_t *val32 = (int32_t *)malloc(sizeof(int32_t));
	
	CU_ASSERT(chidb_DBRecord_getInt32(dbr, 0, val32) == CHIDB_OK);
	CU_ASSERT((*val32) = 21000);
	
	int *len = (int *)malloc(sizeof(int));
	CU_ASSERT(chidb_DBRecord_getStringLength(dbr, 1, len) == CHIDB_OK);
	CU_ASSERT((*len) == 21);
	char *course_title = (char *)malloc(sizeof(char) * (*len));
	CU_ASSERT(chidb_DBRecord_getString(dbr, 1, &(course_title)) == CHIDB_OK);
	CU_ASSERT(strcmp(course_title, "Programming Languages") == 0);
	CU_ASSERT(chidb_DBRecord_getInt8(dbr, 2, val8) == CHIDB_OK);
	printf("VAL8: %i\n", (*val8));
	CU_ASSERT((*val8) == 75);
	CU_ASSERT(chidb_DBRecord_getInt32(dbr, 3, val32) == CHIDB_OK);
	CU_ASSERT((*val32) == 89);
	
	DBRecord *dbr2;
	
	CU_ASSERT(chidb_DBRecord_unpack(&(dbr2), test_dbm->cursors[0].next_cell->fields.tableLeaf.data) == CHIDB_OK);
	
	
	CU_ASSERT(chidb_DBRecord_getInt32(dbr2, 0, val32) == CHIDB_OK);
	CU_ASSERT((*val32) = 23500);
	
	CU_ASSERT(chidb_DBRecord_getStringLength(dbr2, 1, len) == CHIDB_OK);
	CU_ASSERT((*len) == 9);
	char *course_title2 = (char *)malloc(sizeof(char) * (*len));
	CU_ASSERT(chidb_DBRecord_getString(dbr2, 1, &(course_title2)) == CHIDB_OK);
	CU_ASSERT(strcmp(course_title2, "Databases") == 0);
	CU_ASSERT(chidb_DBRecord_getInt8(dbr2, 2, val8) == CHIDB_OK);
	printf("VAL8: %i\n", (*val8));
	CU_ASSERT((*val8) == 42);
	
	free(dbr);
	free(dbr2);
	free(val8);
	free(val16);
	free(val32);
	free(bt);
	free(db);
	free(test_dbm);
}

void test_9_13(void) {
	//DBM_NEXT
}

int init_tests_btree()
{
  CU_pSuite openexistingTests, loadnodeTests, createwriteTests, opennewTests, cellTests, findTests, insertnosplitTests, insertTests, indexTests, dbmTests;
  
  /* add suites to the registry */
  if (
      NULL == (openexistingTests =  CU_add_suite("Step 1a: Opening an existing chidb file", NULL, NULL))	||
      NULL == (loadnodeTests =      CU_add_suite("Step 2: Loading a B-Tree node from the file", NULL, NULL))	||
      NULL == (createwriteTests =   CU_add_suite("Step 3: Creating and writing a B-Tree node to disk", NULL, NULL))	||
      NULL == (opennewTests =       CU_add_suite("Step 1b: Opening a new chidb file", NULL, NULL))	||
      NULL == (cellTests =          CU_add_suite("Step 4: Manipulating B-Tree cells", NULL, NULL))	||
      NULL == (findTests =          CU_add_suite("Step 5: Finding a value in a B-Tree", NULL, NULL))	||
      NULL == (insertnosplitTests = CU_add_suite("Step 6: Insertion into a leaf without splitting", NULL, NULL))	||
      NULL == (insertTests =        CU_add_suite("Step 7: Insertion with splitting", NULL, NULL))	||
      NULL == (indexTests =         CU_add_suite("Step 8: Supporting index B-Trees", NULL, NULL)) ||
      NULL == (dbmTests = 					CU_add_suite("Step 9: Testing DBM commands", NULL, NULL))
      ) 
    {
      CU_cleanup_registry();
      return CU_get_error();
    }
  
  if (
          /*
      (NULL == CU_add_test(openexistingTests, "1a.1", test_1a_1)) ||
      (NULL == CU_add_test(openexistingTests, "1a.2", test_1a_2)) ||
      (NULL == CU_add_test(openexistingTests, "1a.3", test_1a_3)) ||
      
      (NULL == CU_add_test(loadnodeTests, "2.1", test_2_1)) ||
      (NULL == CU_add_test(loadnodeTests, "2.2", test_2_2)) ||
      (NULL == CU_add_test(loadnodeTests, "2.3", test_2_3)) ||
      (NULL == CU_add_test(loadnodeTests, "2.4", test_2_4)) ||
      (NULL == CU_add_test(loadnodeTests, "2.5", test_2_5)) ||
      (NULL == CU_add_test(loadnodeTests, "2.6", test_2_6)) ||
		
      (NULL == CU_add_test(createwriteTests, "3.1", test_3_1)) ||
      (NULL == CU_add_test(createwriteTests, "3.2", test_3_2)) ||
      (NULL == CU_add_test(createwriteTests, "3.3", test_3_3)) ||
      (NULL == CU_add_test(createwriteTests, "3.4", test_3_4)) ||
      (NULL == CU_add_test(createwriteTests, "3.5", test_3_5)) ||
      (NULL == CU_add_test(createwriteTests, "3.6", test_3_6)) ||
      (NULL == CU_add_test(createwriteTests, "3.7", test_3_7)) ||
      (NULL == CU_add_test(createwriteTests, "3.8", test_3_8)) ||
      (NULL == CU_add_test(createwriteTests, "3.9", test_3_9)) ||
      (NULL == CU_add_test(createwriteTests, "3.10", test_3_10)) ||
      
      (NULL == CU_add_test(opennewTests, "1b.1", test_1b_1)) ||
      (NULL == CU_add_test(opennewTests, "1b.2", test_1b_2)) ||
      
      (NULL == CU_add_test(cellTests, "4.1", test_4_1)) ||
      (NULL == CU_add_test(cellTests, "4.2", test_4_2)) ||
      (NULL == CU_add_test(cellTests, "4.3", test_4_3)) ||
      (NULL == CU_add_test(cellTests, "4.4", test_4_4)) ||
      
      (NULL == CU_add_test(findTests, "5.1", test_5_1)) ||
      (NULL == CU_add_test(findTests, "5.2", test_5_2)) ||
      
      (NULL == CU_add_test(insertnosplitTests, "6.1", test_6_1)) ||
      (NULL == CU_add_test(insertnosplitTests, "6.2", test_6_2)) ||
      
      (NULL == CU_add_test(insertTests, "7.1", test_7_1)) ||
      (NULL == CU_add_test(insertTests, "7.2", test_7_2)) ||
      (NULL == CU_add_test(insertTests, "7.3", test_7_3)) ||
      
      (NULL == CU_add_test(indexTests, "8.1", test_8_1)) ||
      (NULL == CU_add_test(indexTests, "8.2", test_8_2)) ||
      (NULL == CU_add_test(indexTests, "8.3", test_8_3)) ||
      */
      /* DBM TESTS */
      (NULL == CU_add_test(dbmTests, "9.1 - DBM_INTEGER, DBM_STRING, and DBM_NULL", test_9_1)) ||
      (NULL == CU_add_test(dbmTests, "9.2 - DBM_EQ", test_9_2)) || 
      (NULL == CU_add_test(dbmTests, "9.3 - DBM_NE", test_9_3)) ||
      (NULL == CU_add_test(dbmTests, "9.4 - DBM_LT", test_9_4)) ||
      (NULL == CU_add_test(dbmTests, "9.5 - DBM_LE", test_9_5)) ||
      (NULL == CU_add_test(dbmTests, "9.6 - DBM_GT", test_9_6)) ||
      (NULL == CU_add_test(dbmTests, "9.7 - DBM_GE", test_9_7)) ||
      (NULL == CU_add_test(dbmTests, "9.8 - DBM_HALT", test_9_8)) ||
      (NULL == CU_add_test(dbmTests, "9.9 - DBM_OPENREAD", test_9_9)) ||
      (NULL == CU_add_test(dbmTests, "9.10 - DBM_OPENWRITE", test_9_10)) ||
      (NULL == CU_add_test(dbmTests, "9.11 - DBM_CLOSE", test_9_11)) ||
      (NULL == CU_add_test(dbmTests, "9.12 - DBM_REWIND", test_9_12)) ||
      (NULL == CU_add_test(dbmTests, "9.13 - DBM_NEXT", test_9_13))
      )
    {
      CU_cleanup_registry();
      return CU_get_error();
    }
  
  
  return CU_get_error();
}
