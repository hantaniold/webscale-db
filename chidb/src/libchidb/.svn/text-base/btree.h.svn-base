#ifndef BTREE_H_
#define BTREE_H_

#include "chidbInt.h"
#include "pager.h"

/* Page header offsets and sizes */

#define PGTYPE_TABLE_INTERNAL (0x05)
#define PGTYPE_TABLE_LEAF (0x0D)
#define PGTYPE_INDEX_INTERNAL (0x02)
#define PGTYPE_INDEX_LEAF (0x0A)

#define PGHEADER_PGTYPE_OFFSET (0)
#define PGHEADER_FREE_OFFSET (1)
#define PGHEADER_NCELLS_OFFSET (3)
#define PGHEADER_CELL_OFFSET (5)
#define PGHEADER_ZERO_OFFSET (7)
#define PGHEADER_RIGHTPG_OFFSET (8)

#define LEAFPG_CELLSOFFSET_OFFSET (8)
#define INTPG_CELLSOFFSET_OFFSET (12)

/* Cell offsets and sizes */

#define TABLEINTCELL_CHILD_OFFSET (0)
#define TABLEINTCELL_KEY_OFFSET (4)

#define TABLELEAFCELL_SIZE_OFFSET (0)
#define TABLELEAFCELL_KEY_OFFSET (4)
#define TABLELEAFCELL_DATA_OFFSET (8)

#define TABLEINTCELL_SIZE (8)
#define TABLELEAFCELL_SIZE_WITHOUTDATA (8)

#define INDEXINTCELL_CHILD_OFFSET (0)
#define INDEXINTCELL_KEYIDX_OFFSET (8)
#define INDEXINTCELL_KEYPK_OFFSET (12)

#define INDEXLEAFCELL_SIZE_OFFSET (0)
#define INDEXLEAFCELL_KEYIDX_OFFSET (4)
#define INDEXLEAFCELL_KEYPK_OFFSET (8)

#define INDEXINTCELL_SIZE (16)
#define INDEXLEAFCELL_SIZE (12)

// Advance declarations
typedef struct BTreeCell BTreeCell;
typedef struct BTreeNode BTreeNode;
void SHOW_ALL_KEYS(BTree *bt);

// BTree Header Struct
struct BTreeHdr
{
    char format_str[16];
    uint16_t page_size;
    char f1[6];
    uint32_t file_change_counter;
    uint32_t f2[3];
    uint32_t schema_version;
    uint32_t f3;
    uint32_t page_cache_size;
    uint32_t f4[2];
    uint32_t user_cookie;
    uint32_t f5[8];
};

/* The BTree struct represent a "B-Tree file". It contains a pointer to the
 * chidb database it is a part of, and a pointer to a Pager, which it will
 * use to access pages on the file */
struct BTree
{
	chidb *db;
	Pager *pager;
};

/* The BTreeNode struct is an in-memory representation of a B-Tree node. Thus,
 * most of the values in this struct are simply a copy, for ease of access, 
 * of what can be found in the raw disk page. When modifying type, free_offset, 
 * n_cells, cells_offset, or right_page, do so in the corresponding field
 * of the BTreeNode variable (the changes will be effective once the BTreeNode
 * is written to disk, using chidb_Btree_writeNode). Modifications of the
 * cell offset array or of the cells should be done directly on the in-memory
 * page returned by the Pager.
 * 
 * See The chidb File Format document for more details on the meaning of each
 * field. 
 */
struct BTreeNode 
{
	MemPage *page;             /* In-memory page returned by the Pager */
	uint8_t type;              /* Type of page  */
	uint16_t free_offset;      /* Byte offset of free space in page */ 
	ncell_t n_cells;           /* Number of cells */
	uint16_t cells_offset;     /* Byte offset of start of cells in page */
	npage_t right_page;        /* Right page (internal nodes only) */
	uint8_t *celloffset_array; /* Pointer to start of cell offset array in the in-memory page */
};

/* BTreeCell is an in-memory representation of a cell. See The chidb File Format 
 * document for more details on the meaning of each field */ 
struct BTreeCell 
{
	uint8_t type;  /* Type of page where this cell is contained */
	key_t key;     /* Key */
	union
	{
		struct
		{
			npage_t child_page;  /* Child page with keys <= key */
		} tableInternal;
		struct
		{
			uint32_t data_size;  /* Number of bytes of data stored in this cell */
			uint8_t *data;       /* Pointer to in-memory copy of data stored in this cell */
		} tableLeaf;
		struct
		{
			key_t keyPk;         /* Primary key of row where the indexed field is equal to key */
			npage_t child_page;  /* Child page with keys < key */
		} indexInternal;
		struct
		{
			key_t keyPk;         /* Primary key of row where the indexed field is equal to key */
		} indexLeaf;
	} fields;
};

 
int chidb_Btree_open(const char *filename, chidb *db, BTree **bt);
int chidb_Btree_close(BTree *bt);

int chidb_Btree_getNodeByPage(BTree *bt, npage_t npage, BTreeNode **node);
int chidb_Btree_freeMemNode(BTree *bt, BTreeNode *btn);

int chidb_Btree_newNode(BTree *bt, npage_t *npage, uint8_t type);
int chidb_Btree_initEmptyNode(BTree *bt, npage_t npage, uint8_t type);
int chidb_Btree_writeNode(BTree *bt, BTreeNode *node);

int chidb_Btree_getCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell);
int chidb_Btree_insertCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell);

int chidb_Btree_find(BTree *bt, npage_t nroot, key_t key, uint8_t **data, uint16_t *size);

int chidb_Btree_insertInTable(BTree *bt, npage_t nroot, key_t key, uint8_t *data, uint16_t size);
int chidb_Btree_insertInIndex(BTree *bt, npage_t nroot, key_t keyIdx, key_t keyPk);
int chidb_Btree_insert(BTree *bt, npage_t nroot, BTreeCell *btc);
int chidb_Btree_insertNonFull(BTree *bt, npage_t npage, BTreeCell *btc);
int chidb_Btree_split(BTree *bt, npage_t npage_parent, npage_t npage_child, ncell_t parent_cell, npage_t *npage_child2);


#endif /*BTREE_H_*/
