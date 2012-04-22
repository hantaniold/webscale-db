/*****************************************************************************
 * 
 *																 chidb 
 * 
 * This module contains functions to manipulate a B-Tree file. In this context,
 * "BTree" refers not to a single B-Tree but to a "file of B-Trees" ("chidb 
 * file" and "file of B-Trees" are essentially equivalent terms). 
 * 
 * However, this module does *not* read or write to the database file directly.
 * All read/write operations must be done through the pager module.
 *
 *
 * 2009, 2010 Borja Sotomayor - http://people.cs.uchicago.edu/~borja/
\*****************************************************************************/ 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <chidbInt.h>
#include "btree.h"
#include "record.h"
#include "pager.h"
#include "util.h"

void SHOW_ALL_KEYS_AT_NODE(BTreeNode *node)
{                                                                               
  for (int i=0; i<node->n_cells; i++) {
    BTreeCell c;
    chidb_Btree_getCell(node,i,&c);
    //if (c.type != 0x05)
    //	return;
    fprintf(stdout,"%i: KEY %i  ",i,c.key);
    if (c.type == 0x05)
    	fprintf(stdout,"Child %i",c.fields.tableInternal.child_page);
	if (c.type == 0x02)
		fprintf(stdout,"Child %i   Key2 %i",c.fields.indexInternal.child_page,c.fields.indexInternal.keyPk);
	if (c.type == 0x0a)
		fprintf(stdout,"Key2 %i",c.fields.indexLeaf.keyPk);
    fprintf(stdout,"\n");
  }
}

void SHOW_ALL_KEYS(BTree *bt)
{
  Pager *pgr = bt->pager;
  npage_t n = pgr->n_pages;
  for (int i=1; i<=n; i++) {
    BTreeNode *node;
    chidb_Btree_getNodeByPage(bt,i,&node);
    //if (node->type == 0x05)
    fprintf(stdout,"SHOW_ALL_KEYS AT PAGE %i (Type %x, RightPage: %i)\n", i, node->type, node->right_page);
    SHOW_ALL_KEYS_AT_NODE(node);
    //if (node->type == 0x05)
    fprintf(stdout,"----\n");
  }
}


/* Open a B-Tree file
 * 
 * This function opens a database file and verifies that the file
 * header is correct. If the file is empty (which will happen
 * if the pager is given a filename for a file that does not exist)
 * then this function will (1) initialize the file header using
 * the default page size and (2) create an empty table leaf node
 * in page 1.
 * 
 * Parameters
 * - filename: Database file (might not exist)
 * - db: A chidb struct. Its bt field must be set to the newly
 *			 created BTree.
 * - bt: An out parameter. Used to return a pointer to the
 *			 newly created BTree.
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECORRUPTHEADER: Database file contains an invalid header
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_open(const char *filename, chidb *db, BTree **bt)
{
    *bt = malloc(sizeof(BTree));
    Pager *pager;

    /* Open the file */
    int file_open = chidb_Pager_open(&pager, filename);
    if(file_open == CHIDB_ENOMEM) {
        return CHIDB_ENOMEM;
    } else if(file_open == CHIDB_EIO) {
        return CHIDB_EIO;
    }

    (*bt)->pager = pager;
    pager->n_pages = 0;

    /* Set page size */
    chidb_Pager_setPageSize(pager, DEFAULT_PAGE_SIZE);

    /* Load the header */
    uint8_t *buf = calloc(100, sizeof(uint8_t));
    int hdr_loaded = chidb_Pager_readHeader(pager, buf);
    if(hdr_loaded == CHIDB_NOHEADER) {
        /* No header exists; create one now */
        struct BTreeHdr *hdr = calloc(1, sizeof(struct BTreeHdr));
        strcpy(hdr->format_str, "SQLite format 3");
        put2byte((unsigned char *) &(hdr->page_size), DEFAULT_PAGE_SIZE);
        hdr->f1[0] = 0x01;
        hdr->f1[1] = 0x01;
        hdr->f1[2] = 0x00;
        hdr->f1[3] = 0x40;
        hdr->f1[4] = 0x20;
        hdr->f1[5] = 0x20;
        put4byte((unsigned char *) &(hdr->f3), 0x00000001);
        put4byte((unsigned char *) &(hdr->page_cache_size), 20000);
        put4byte((unsigned char *) &(hdr->f4[1]), 0x00000001);

        /* Create an empty leaf node */
        npage_t npage;
        int err = chidb_Btree_newNode(*bt, &npage, 0x0d);
        if(err != CHIDB_OK)
            return err;
        err = chidb_Btree_initEmptyNode(*bt, npage, 0x0d);
        if(err != CHIDB_OK)
            return err;

        /* Write the header to the new node */
        BTreeNode *btn;
        chidb_Btree_getNodeByPage(*bt, npage, &btn);
        memmove(btn->celloffset_array, btn->celloffset_array - 100, sizeof(uint16_t));
        memcpy(btn->page->data, hdr, sizeof(struct BTreeHdr));
        chidb_Btree_writeNode(*bt, btn);
        chidb_Btree_freeMemNode(*bt, btn);
        free(buf);
        free(hdr);

        db->bt = *bt;
        
    } else {

        /* Verify the header */
        int is_valid = 1;
        struct BTreeHdr *hdr = (struct BTreeHdr *) buf;
        if(strcmp(hdr->format_str, "SQLite format 3")) {
            is_valid = 0;
        }
        if(hdr->f1[0] != 0x01 || hdr->f1[1] != 0x01 || hdr->f1[2] != 0x00 || hdr->f1[3] != 0x40 || hdr->f1[4] != 0x20 || hdr->f1[5] != 0x20) {
            is_valid = 0;
        }
        if(hdr->f2[0] != 0 || hdr->f2[1] != 0) {
            is_valid = 0;
        }
        if(get4byte((const uint8_t *) &(hdr->f3)) != 1) {
            is_valid = 0;
        }
        if(get4byte((const uint8_t *) &(hdr->page_cache_size)) != 20000) {
            is_valid = 0;
        }
        if(hdr->f4[0] != 0 || get4byte((const uint8_t *) &(hdr->f4[1])) != 1) {
            is_valid = 0;
        }
        if(hdr->f5[0] != 0) {
            is_valid = 0;
        }
        
        if(!is_valid) {
            return CHIDB_ECORRUPTHEADER;
        }
        free(buf);

        db->bt = *bt;
    }

    return CHIDB_OK;
}


/* Close a B-Tree file
 * 
 * This function closes a database file, freeing any resource
 * used in memory, such as the pager.
 * 
 * Parameters
 * - bt: B-Tree file to close
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_close(BTree *bt)
{
    int result = chidb_Pager_close(bt->pager);
    free(bt);

    return result;
}


/* Loads a B-Tree node from disk
 * 
 * Reads a B-Tree node from a page in the disk. All the information regarding
 * the node is stored in a BTreeNode struct (see header file for more details
 * on this struct). *This is the only function that can allocate memory for
 * a BTreeNode struct*. Always use chidb_Btree_freeMemNode to free the memory
 * allocated for a BTreeNode (do not use free() directly on a BTreeNode variable)
 * Any changes made to a BTreeNode variable will not be effective in the database
 * until chidb_Btree_writeNode is called on that BTreeNode.
 * 
 * Parameters
 * - bt: B-Tree file
 * - npage: Page of node to load
 * - btn: Out parameter. Used to return a pointer to newly created BTreeNode
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EPAGENO: The provided page number is not valid
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_getNodeByPage(BTree *bt, npage_t npage, BTreeNode **btn)
{
    // Read page from memory
    *btn = calloc(1, sizeof(BTreeNode));
    if (*btn == NULL)
        return CHIDB_ENOMEM;
    MemPage *page;
    int err = chidb_Pager_readPage(bt->pager, npage, &page);
    if (err == CHIDB_EPAGENO || err == CHIDB_ENOMEM)
        return err;

    // Determine offset and load fields
    int offset = (page->npage == 1) ? 100 : 0;
    (*btn)->type = *(page->data + offset);
    (*btn)->free_offset = get2byte(page->data + 1 + offset);
    (*btn)->n_cells = get2byte(page->data + 3 + offset);
    (*btn)->cells_offset = get2byte(page->data + 5 + offset);
    if((*btn)->type == 0x05 || (*btn)->type == 0x02) {
        (*btn)->right_page = get4byte(page->data + 8 + offset);
        (*btn)->celloffset_array = page->data + 12 + offset;
    } else {
        (*btn)->celloffset_array = page->data + 8 + offset;
    }

    (*btn)->page = page;

	return CHIDB_OK;
}


/* Frees the memory allocated to an in-memory B-Tree node
 * 
 * Frees the memory allocated to an in-memory B-Tree node, and 
 * the in-memory page returned by the pages (stored in the
 * "page" field of BTreeNode)
 * 
 * Parameters
 * - bt: B-Tree file
 * - btn: BTreeNode to free
 * 
 * Return
 * - CHIDB_OK: Operation successful
 */
int chidb_Btree_freeMemNode(BTree *bt, BTreeNode *btn)
{
    chidb_Pager_releaseMemPage(bt->pager, btn->page);
    free(btn);
	return CHIDB_OK;
}


/* Create a new B-Tree node
 * 
 * Allocates a new page in the file and initializes it as a B-Tree node.
 * 
 * Parameters
 * - bt: B-Tree file
 * - npage: Out parameter. Returns the number of the page that
 *					was allocated.
 * - type: Type of B-Tree node (PGTYPE_TABLE_INTERNAL, PGTYPE_TABLE_LEAF,
 *				 PGTYPE_INDEX_INTERNAL, or PGTYPE_INDEX_LEAF)
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_newNode(BTree *bt, npage_t *npage, uint8_t type)
{
    chidb_Pager_allocatePage(bt->pager, npage);
    return chidb_Btree_initEmptyNode(bt, *npage, type);
}



/* Initialize a B-Tree node
 * 
 * Initializes a database page to contain an empty B-Tree node. The
 * database page is assumed to exist and to have been already allocated
 * by the pager.
 * 
 * Parameters
 * - bt: B-Tree file
 * - npage: Database page where the node will be created.
 * - type: Type of B-Tree node (PGTYPE_TABLE_INTERNAL, PGTYPE_TABLE_LEAF,
 *				 PGTYPE_INDEX_INTERNAL, or PGTYPE_INDEX_LEAF)
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_initEmptyNode(BTree *bt, npage_t npage, uint8_t type)
{
   // Allocate new node and page
   struct BTreeNode *node = calloc(1, sizeof(struct BTreeNode));
   if(node == NULL)
       return CHIDB_ENOMEM;
   MemPage *page;
   int err = chidb_Pager_readPage(bt->pager, npage, &page);
   if (err == CHIDB_EPAGENO || err == CHIDB_ENOMEM)
        return err;
   node->page = page;
   node->type = type;

   // Determine various offsets
   int offset = (npage == 1) ? 100 : 0;
   switch(type) {
       case 0x05:
       case 0x02:
           node->free_offset = (uint16_t) (12 + offset);
           break;
       case 0x0d:
       case 0x0a:
           node->free_offset = (uint16_t) (8 + offset);
           break;
   }
   node->n_cells = (uint16_t) 0;
   node->cells_offset = (uint16_t) DEFAULT_PAGE_SIZE;

  if (type == 0x02 || type == 0x05) {
       node->celloffset_array = (uint8_t *) node + 12; 
   } else {
       node->celloffset_array = (uint8_t *) node + 8;
   }

   err = chidb_Btree_writeNode(bt, node);
   if(err != CHIDB_OK)
       return err;

   return CHIDB_OK;
}



/* Write an in-memory B-Tree node to disk
 * 
 * Writes an in-memory B-Tree node to disk. To do this, we need to update
 * the in-memory page according to the chidb page format. Since the cell
 * offset array and the cells themselves are modified directly on the
 * page, the only thing to do is to store the values of "type",
 * "free_offset", "n_cells", "cells_offset" and "right_page" in the
 * in-memory page.
 * 
 * Parameters
 * - bt: B-Tree file
 * - btn: BTreeNode to write to disk
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_writeNode(BTree *bt, BTreeNode *btn)
{
    int offset = (btn->page->npage == 1) ? 100 : 0;
    *(btn->page->data + offset) = btn->type;
    put2byte(btn->page->data + offset + 1, btn->free_offset);
    put2byte(btn->page->data + offset + 3, btn->n_cells);
    put2byte(btn->page->data + offset + 5, btn->cells_offset);
    *(btn->page->data + offset + 7) = 0;
    if(btn->type == 0x02 || btn->type == 0x05)
        put4byte(btn->page->data + offset + 8, btn->right_page);

    chidb_Pager_writePage(bt->pager, btn->page);

	return CHIDB_OK;
}


/* Read the contents of a cell
 * 
 * Reads the contents of a cell from a BTreeNode and stores them in a BTreeCell.
 * This involves the following:
 *	1. Find out the offset of the requested cell.
 *	2. Read the cell from the in-memory page, and parse its
 *		 contents (refer to The chidb File Format document for
 *		 the format of cells).
 *	
 * Parameters
 * - btn: BTreeNode where cell is contained
 * - ncell: Cell number
 * - cell: BTreeCell where contents must be stored.
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECELLNO: The provided cell number is invalid
 */
int chidb_Btree_getCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell)
{	
	if(btn->n_cells < ncell)
		return CHIDB_ECELLNO;

    // Get pointer to the cell
    uint16_t cell_offset = get2byte(btn->celloffset_array + (2 * ncell));
    uint8_t *cell_ptr = (uint8_t *)(btn->page->data) + cell_offset;

    // Load fields
	cell->type = btn->type;
	switch(cell->type) {
		case 0x05: // Internal Table Page
            getVarint32((const uint8_t *)(cell_ptr + 4), (uint32_t *)&(cell->key));
            cell->fields.tableInternal.child_page = get4byte(cell_ptr);
			break;
		case 0x0d: // Leaf Table Page
            getVarint32((const uint8_t *)(cell_ptr + 4), (uint32_t *)&(cell->key));
            getVarint32((const uint8_t *)(cell_ptr), (uint32_t *)&(cell->fields.tableLeaf.data_size));
			cell->fields.tableLeaf.data = cell_ptr + 8;
			break;
		case 0x02: // Internal Index Page
            cell->key = get4byte(cell_ptr + 8);
			cell->fields.indexInternal.keyPk = get4byte(cell_ptr + 12);
            cell->fields.indexInternal.child_page = get4byte(cell_ptr);
			break;
		case 0x0a: // Leaf Index Page
            cell->key = get4byte(cell_ptr + 4);
			cell->fields.indexLeaf.keyPk = get4byte(cell_ptr + 8);
			break;
	}

	return CHIDB_OK;
}


/* Insert a new cell into a B-Tree node
 * 
 * Inserts a new cell into a B-Tree node at a specified position ncell.
 * This involves the following:
 *	1. Add the cell at the top of the cell area. This involves "translating"
 *		 the BTreeCell into the chidb format (refer to The chidb File Format 
 *		 document for the format of cells).
 *	2. Modify cells_offset in BTreeNode to reflect the growth in the cell area.
 *	3. Modify the cell offset array so that all values in positions >= ncell
 *		 are shifted one position forward in the array. Then, set the value of
 *		 position ncell to be the offset of the newly added cell.
 *
 * This function assumes that there is enough space for this cell in this node.
 *	
 * Parameters
 * - btn: BTreeNode to insert cell in
 * - ncell: Cell number
 * - cell: BTreeCell to insert.
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECELLNO: The provided cell number is invalid
 */
int chidb_Btree_insertCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell)
{
    // Create a data array and assemble the new cell there
	int cellsize;
	uint8_t data[16];
	switch(cell->type) {
		case 0x05: // Internal Table Page
			cellsize = 8;
            put4byte(data, cell->fields.tableInternal.child_page);
            putVarint32(data + 4, cell->key);
			break;
		case 0x0d: // Leaf Table Page
			cellsize = 8 + cell->fields.tableLeaf.data_size;
            putVarint32(data, cell->fields.tableLeaf.data_size);
            putVarint32(data + 4, cell->key);
			break;
		case 0x02: // Internal Index Page
			cellsize = 16;
            put4byte(data, cell->fields.indexInternal.child_page);
			data[4] = 0x0b;
			data[5] = 0x03;
			data[6] = 0x04;
			data[7] = 0x04;
            put4byte(data + 8, cell->key);
            put4byte(data + 12, cell->fields.indexInternal.keyPk);
			break;
		case 0x0a: // Leaf Index Page
			cellsize = 12;
			data[0] = 0x0b;
			data[1] = 0x03;
			data[2] = 0x04;
			data[3] = 0x04;
            put4byte(data + 4, cell->key);
            put4byte(data + 8, cell->fields.indexLeaf.keyPk);
			break;
	}

    // Store the new cell in the actual mempage
    uint16_t cell_start = btn->cells_offset - cellsize;
    memcpy(btn->page->data + cell_start, data, cellsize);
    if(cell->type == 0x0d) {
        memcpy(btn->page->data + cell_start + 8, cell->fields.tableLeaf.data, cell->fields.tableLeaf.data_size);
    }

    // Update other necessary data
    btn->cells_offset -= cellsize;
    for(int i = (btn->n_cells - 1) * 2; i >= ncell * 2; i-=2) {
        btn->celloffset_array[i+2] = btn->celloffset_array[i];
        btn->celloffset_array[i+3] = btn->celloffset_array[i+1];
    }
    btn->free_offset += sizeof(uint16_t);
    put2byte(&(btn->celloffset_array[ncell*2]), btn->cells_offset);
    btn->n_cells++;

	return CHIDB_OK;
}


/* Find an entry in a table B-Tree
 * 
 * Finds the data associated for a given key in a table B-Tree
 * 
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want search in
 * - key: Entry key
 * - data: Out-parameter where a copy of the data must be stored
 * - size: Number of bytes of data
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: No entry with the given key was found
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_find(BTree *bt, npage_t nroot, key_t key, 
		     uint8_t **data, uint16_t *size) {
	// Get root node
	BTreeNode *btn;
	int err;
	err = chidb_Btree_getNodeByPage(bt, nroot, &btn);
	if(err != CHIDB_OK)
		return err;

	// Recursively search through the tree
	BTreeCell *cell = malloc(sizeof(BTreeCell));
    err = CHIDB_ENOTFOUND;
	for(int i = 0; i <= btn->n_cells; i++) {
		chidb_Btree_getCell(btn, (ncell_t) i, cell);
		switch(cell->type) {
			case 0x05: // Table internal
                if(i != btn->n_cells) {
			    	if(cell->key >= key) {
                        err = chidb_Btree_find(bt, cell->fields.tableInternal.child_page, key, data, size);
			    	}
                } else {
    				// Go to right table page if all other child pages have been exhausted
    				err = chidb_Btree_find(bt, btn->right_page, key, data, size);
                }
				break;
			case 0x0d: // Table leaf
				if(cell->key == key) {
                    *data = calloc(cell->fields.tableLeaf.data_size, sizeof(char));
                    memcpy(*data, cell->fields.tableLeaf.data, cell->fields.tableLeaf.data_size);
                    memcpy(size, &(cell->fields.tableLeaf.data_size), sizeof(uint16_t));
                    err = CHIDB_OK;
				}
				break;
			case 0x02: // Index internal
			case 0x0a: // Index leaf
                // Code is implemented in testing suite, not needed as part of Project 1
				break;
		}
        if (!err)
            break;
	}
    free(cell);
    chidb_Btree_freeMemNode(bt, btn);

	// Return result
	return err;
}


	
/* Insert an entry into a table B-Tree
 *
 * This is a convenience function that wraps around chidb_Btree_insert.
 * It takes a key and data, and creates a BTreeCell that can be passed
 * along to chidb_Btree_insert.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *					this entry in.
 * - key: Entry key
 * - data: Pointer to data we want to insert
 * - size: Number of bytes of data
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertInTable(BTree *bt, npage_t nroot, key_t key, 
			      uint8_t *data, uint16_t size)
{
	BTreeCell *cell = malloc(sizeof(BTreeCell));
	cell->type = 0x0d;
	cell->key = key;
	cell->fields.tableLeaf.data_size = size;
	cell->fields.tableLeaf.data = data;
	
    int err = chidb_Btree_insert(bt, nroot, cell);
    free(cell);

    return err;
}


/* Insert an entry into an index B-Tree
 *
 * This is a convenience function that wraps around chidb_Btree_insert.
 * It takes a KeyIdx and a KeyPk, and creates a BTreeCell that can be passed
 * along to chidb_Btree_insert.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *					this entry in.
 * - keyIdx: See The chidb File Format.
 * - keyPk: See The chidb File Format.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertInIndex(BTree *bt, npage_t nroot, key_t keyIdx, key_t keyPk)
{
	BTreeCell *cell = malloc(sizeof(BTreeCell));
	cell->type = 0x0a;
	cell->key = keyIdx;
	cell->fields.indexLeaf.keyPk = keyPk;

	int err = chidb_Btree_insert(bt, nroot, cell);
    free(cell);

    return err;
}


/* Insert a BTreeCell into a B-Tree
 *
 * The chidb_Btree_insert and chidb_Btree_insertNonFull functions
 * are responsible for inserting new entries into a B-Tree, although
 * chidb_Btree_insertNonFull is the one that actually does the
 * insertion. chidb_Btree_insert, however, first checks if the root
 * has to be split (a splitting operation that is different from
 * splitting any other node). If so, chidb_Btree_split is called
 * before calling chidb_Btree_insertNonFull.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *					this cell in.
 * - btc: BTreeCell to insert into B-Tree
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insert(BTree *bt, npage_t nroot, BTreeCell *btc)
{
    int err;

    // Check if the root node has space for another cell
    BTreeNode *btn;
    err = chidb_Btree_getNodeByPage(bt, nroot, &btn);
    if(err != CHIDB_OK)
        return err;
    uint32_t sizeOfFreeSpace = btn->cells_offset - btn->free_offset;
    uint32_t sizeOfNewCell;
    switch(btc->type) {
        case 0x05: // Table internal
            sizeOfNewCell = 10; // 8 bytes for cell, 2 for cell offset array entry
            break;
        case 0x0d: // Table leaf
            sizeOfNewCell = 10 + btc->fields.tableLeaf.data_size;
            break;
        case 0x02: // Index internal
            sizeOfNewCell = 18;
            break;
        case 0x0a: // Index leaf
            sizeOfNewCell = 14;
            break;
    }
    
    if((int32_t)sizeOfFreeSpace - (int32_t)sizeOfNewCell >= 0) {
        err = chidb_Btree_insertNonFull(bt, nroot, btc);
    } else {
        // Allocate a new page in memory
	npage_t npage;
    if(btc->type == 0x0d) {
    	err = chidb_Btree_newNode(bt, &npage, 0x05);
    	err = chidb_Btree_initEmptyNode(bt, npage, 0x05);
    } else if(btc->type == 0x0a) {
        err = chidb_Btree_newNode(bt, &npage, 0x02);
        err = chidb_Btree_initEmptyNode(bt, npage, 0x02);
    }

    // Copy root node
	BTreeNode *newRootNode;
	err = chidb_Btree_getNodeByPage(bt, nroot, &newRootNode);
	newRootNode->free_offset -= (newRootNode->n_cells * 2);
	newRootNode->cells_offset = DEFAULT_PAGE_SIZE;
	newRootNode->n_cells = 0;
    if (newRootNode->type == 0x0d) {
        newRootNode->type = 0x05;
        newRootNode->free_offset += 4;
    } else if (newRootNode->type == 0x0a) {
        newRootNode->type = 0x02;
        newRootNode->free_offset += 4;
    }

	btn->page->npage = npage;
    if(nroot == 1) {
        memmove(btn->page->data, btn->page->data + 100, btn->free_offset - 100);
        btn->free_offset -= 100;
        btn->celloffset_array -= 100;
    }

	chidb_Btree_writeNode(bt, newRootNode);
	chidb_Btree_writeNode(bt, btn);

    // Split the old root node
	npage_t rightPage;
	err = chidb_Btree_split(bt, nroot, npage, 0, &rightPage);

    // Attach the right page to the new root
    BTreeNode *root;
    chidb_Btree_getNodeByPage(bt, nroot, &root);
    root->right_page = rightPage;
    chidb_Btree_writeNode(bt, root);

    err = chidb_Btree_insertNonFull(bt, nroot, btc);
    }

    return err;
}

/* Insert a BTreeCell into a non-full B-Tree node
 *
 * chidb_Btree_insertNonFull inserts a BTreeCell into a node that is
 * assumed not to be full (i.e., does not require splitting). If the
 * node is a leaf node, the cell is directly added in the appropriate
 * position according to its key. If the node is an internal node, the
 * function will determine what child node it must insert it in, and
 * calls itself recursively on that child node. However, before doing so
 * it will check if the child node is full or not. If it is, then it will
 * have to be split first.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *					this cell in.
 * - btc: BTreeCell to insert into B-Tree
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertNonFull(BTree *bt, npage_t npage, BTreeCell *btc)
{
    // Find the page of the node where the cell is to be inserted
    BTreeNode *btn = malloc(sizeof(BTreeNode));
    BTreeCell *cell = malloc(sizeof(BTreeCell));
    int err;
    chidb_Btree_getNodeByPage(bt, npage, &btn);

    // Get the cell size
    uint32_t btcSize;
    switch(btc->type) {
        case 0x05:
            btcSize = 10;
            break;
        case 0x0d:
            btcSize = 10 + btc->fields.tableLeaf.data_size;
            break;
        case 0x02:
            btcSize = 18;
            break;
        case 0x0a:
            btcSize = 14;
            break;
    }

    switch(btn->type) {
        case 0x05: // Table internal
        {
            int found = 0;

            // Search through all of the children other than the right page
            for(int i = 0; i < btn->n_cells; i++) {
                chidb_Btree_getCell(btn, (ncell_t) i, cell);

                // If the cell can be inserted here...
                if(cell->key >= btc->key) {
                    found = 1;
                    BTreeNode *childNode;
                    chidb_Btree_getNodeByPage(bt, cell->fields.tableInternal.child_page, &childNode);

                    // Determine whether the child node has to be split
                    if((int16_t)childNode->cells_offset - (int16_t)childNode->free_offset - (int16_t)btcSize < 0) {
                        npage_t childPage;
                        err = chidb_Btree_split(bt, npage, cell->fields.tableInternal.child_page, (ncell_t)i, &childPage);

                        // Fix the child page of the old cell
                        chidb_Btree_getNodeByPage(bt, npage, &btn);
                        uint16_t cell_offset = get2byte(btn->celloffset_array + (2 * (i+1)));
                        uint8_t *cell_ptr = (uint8_t *)(btn->page->data) + cell_offset;
                        put4byte(cell_ptr, childPage);
                        chidb_Btree_writeNode(bt, btn);
                        BTreeCell *newCell = malloc(sizeof(BTreeCell));
                        chidb_Btree_getCell(btn, i, newCell);

                        // Continue with insertion
                        if(newCell->key >= btc->key)
                        	err = chidb_Btree_insertNonFull(bt, cell->fields.tableInternal.child_page, btc);
                        else
                        	err = chidb_Btree_insertNonFull(bt, childPage, btc);
                        free(newCell);
                        break;
                    } else {
                        err = chidb_Btree_insertNonFull(bt, cell->fields.tableInternal.child_page, btc);
                        break;
                    }
                    if(err != CHIDB_OK)
                        return err;
                }
            }

            // Go to the right page if none of the other children were a match
            if(!found) {
                BTreeNode *childNode;
                chidb_Btree_getNodeByPage(bt, btn->right_page, &childNode);

                // Determine whether the child node has to be split
                if((int16_t)childNode->cells_offset - (int16_t)childNode->free_offset - (int16_t)btcSize < 0) {
                    npage_t childPage;
					npage_t tempRightPage = btn->right_page;
                    err = chidb_Btree_split(bt, npage, btn->right_page, btn->n_cells, &childPage);

                    // Fix the child page of the new cell
                    chidb_Btree_getNodeByPage(bt, npage, &btn);
                    btn->right_page = childPage;
                    err = chidb_Btree_writeNode(bt, btn);
					BTreeCell *newCell = malloc(sizeof(BTreeCell));					
					chidb_Btree_getCell(btn, btn->n_cells - 1, newCell);
                    
                    // Continue with insertion
                    if(newCell->key >= btc->key)
                      	err = chidb_Btree_insertNonFull(bt, tempRightPage, btc);
                    else
                      	err = chidb_Btree_insertNonFull(bt, childPage, btc);
                    free(newCell);
                } else {
                    err = chidb_Btree_insertNonFull(bt, btn->right_page, btc);
                }
            }

            if(err != CHIDB_OK)
                return err;
            break;
        }
        case 0x0d: // Table leaf
		case 0x0a: // Index leaf
        {
			int i;

            // Find the position where the cell should be inserted
            for(i = 0; i < btn->n_cells; i++) {
                chidb_Btree_getCell(btn, (ncell_t) i, cell);
                if(cell->key > btc->key) {
                    // Stop searching when found
                    break;
                } else if(cell->key == btc->key) {
                    return CHIDB_EDUPLICATE;
                }
	    	}
            // Insert the cell
	    	err = chidb_Btree_insertCell(btn, (ncell_t) i, btc);
           break;
        }
        case 0x02: // Index internal
        {
            int found = 0;

            // Search through all the children other than the right page
            for(int i = 0; i < btn->n_cells; i++) {
                chidb_Btree_getCell(btn, (ncell_t) i, cell);

                // If the cell can be inserted here...
                if(cell->key > btc->key) {
                    found = 1;
                    BTreeNode *childNode;
                    chidb_Btree_getNodeByPage(bt, cell->fields.indexInternal.child_page, &childNode);

                    // Determine whether the child node has to be split
                    if((int16_t)childNode->cells_offset - (int16_t)childNode->free_offset - (int16_t)btcSize < 0) {
                        npage_t childPage;
                        err = chidb_Btree_split(bt, npage, cell->fields.indexInternal.child_page, (ncell_t)i, &childPage);

                        // Fix the child page of the old cell
                        chidb_Btree_getNodeByPage(bt, npage, &btn);
                        uint16_t cell_offset = get2byte(btn->celloffset_array + (2 * (i+1)));
                        uint8_t *cell_ptr = (uint8_t *)(btn->page->data) + cell_offset;
                        put4byte(cell_ptr, childPage);
                        chidb_Btree_writeNode(bt, btn);
                        BTreeCell *newCell = malloc(sizeof(BTreeCell));
                        chidb_Btree_getCell(btn, i, newCell);

                        // Continue with insertion
                        if(newCell->key > btc->key)
                        	err = chidb_Btree_insertNonFull(bt, cell->fields.indexInternal.child_page, btc);
                        else
                        	err = chidb_Btree_insertNonFull(bt, childPage, btc);
                        free(newCell);
                        break;
                    } else {
                        err = chidb_Btree_insertNonFull(bt, cell->fields.indexInternal.child_page, btc);
                        break;
                    }
                    if(err != CHIDB_OK)
                        return err;
                }
            }

            // Go to the right page if none of the other children were a match
            if(!found) {
                BTreeNode *childNode;
                chidb_Btree_getNodeByPage(bt, btn->right_page, &childNode);

                // Determine whether the child node has to be split
                if((int16_t)childNode->cells_offset - (int16_t)childNode->free_offset - (int16_t)btcSize < 0) {
                    npage_t childPage;
					npage_t tempRightPage = btn->right_page;
                    err = chidb_Btree_split(bt, npage, btn->right_page, btn->n_cells, &childPage);

                    // Fix the child page of the new cell
                    chidb_Btree_getNodeByPage(bt, npage, &btn);
                    btn->right_page = childPage;
                    err = chidb_Btree_writeNode(bt, btn);
					BTreeCell *newCell = malloc(sizeof(BTreeCell));					
					chidb_Btree_getCell(btn, btn->n_cells - 1, newCell);

                    // Continue with insertion
                    if(newCell->key > btc->key)
                      	err = chidb_Btree_insertNonFull(bt, tempRightPage, btc);
                    else
                      	err = chidb_Btree_insertNonFull(bt, childPage, btc);
                    free(newCell);
                } else {
                    err = chidb_Btree_insertNonFull(bt, btn->right_page, btc);
                }
            }
            if(err != CHIDB_OK)
                return err;
            break;
        }
	}

    err = chidb_Btree_writeNode(bt, btn);

    chidb_Btree_freeMemNode(bt, btn);
    free(cell);

    return CHIDB_OK;
}


/* Split a B-Tree node
 *
 * Splits a B-Tree node N. This involves the following:
 * - Find the median cell in N.
 * - Create a new B-Tree node M.
 * - Move the cells before the median cell to M (if the
 *	 cell is a table leaf cell, the median cell is moved too)
 * - Add a cell to the parent (which, by definition, will be an
 *	 internal page) with the median key and the page number of M.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage_parent: Page number of the parent node
 * - npage_child: Page number of the node to split
 * - parent_ncell: Position in the parent where the new cell will
 *								 be inserted.
 * - npage_child2: Out parameter. Used to return the page of the new child node.
 * - btc: BTreeCell to insert into B-Tree
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_split(BTree *bt, npage_t npage_parent, npage_t npage_child, 
		      ncell_t parent_ncell, npage_t *npage_child2)
{
	int err;
    BTreeNode *nodeToSplit, *leftNode, *rightNode;

	// Get the median cell in the node to be split
	err = chidb_Btree_getNodeByPage(bt, npage_child, &nodeToSplit);
	if(err != CHIDB_OK)
		return err;
	ncell_t median = (nodeToSplit->n_cells) / 2;
	BTreeCell *middleCell = malloc(sizeof(BTreeCell));
	err = chidb_Btree_getCell(nodeToSplit, median, middleCell);
	if(err != CHIDB_OK)
		return err;

    // Setting up the left node
	npage_t leftPage;
	leftPage = npage_child;
	err = chidb_Btree_getNodeByPage(bt, leftPage, &leftNode);
        leftNode->free_offset -= (leftNode->n_cells * 2);
        leftNode->cells_offset = DEFAULT_PAGE_SIZE;
        leftNode->n_cells = 0;
    
    // Create the new right node
	npage_t rightPage;
	err = chidb_Btree_newNode(bt, &rightPage, nodeToSplit->type);
    if(err != CHIDB_OK)
		return err;
	err = chidb_Btree_getNodeByPage(bt, rightPage, &rightNode);
	if(err != CHIDB_OK)
		return err;
    
    *npage_child2 = rightPage;

	// Move cells to new nodes
	int numCellsToMove = (nodeToSplit->type == 0x0d) ? (int)median + 1 : (int)median;
    BTreeCell *cellToMove = malloc(sizeof(BTreeCell));
	for(int i = 0; i < numCellsToMove; i++) {
		err = chidb_Btree_getCell(nodeToSplit, (ncell_t)i, cellToMove);
		if(err != CHIDB_OK)
			return err;
		err = chidb_Btree_insertCell(leftNode, (ncell_t)i, cellToMove);
		if(err != CHIDB_OK)
			return err;
	}
    numCellsToMove += ((nodeToSplit->type == 0x0d) ? 0 : 1);
    for(int i = numCellsToMove; i < nodeToSplit->n_cells; i++) {
    	err = chidb_Btree_getCell(nodeToSplit, (ncell_t)i, cellToMove);
        if(err != CHIDB_OK)
            return err;
        err = chidb_Btree_insertCell(rightNode, (ncell_t)(i - numCellsToMove), cellToMove);
        if(err != CHIDB_OK)
            return err;
    }
    free(cellToMove);

	// Add a new cell to the parent
    BTreeNode *parentNode;
    err = chidb_Btree_getNodeByPage(bt, npage_parent, &parentNode);

	BTreeCell *newcell = malloc(sizeof(BTreeCell));
	newcell->key = middleCell->key;
	newcell->type = parentNode->type;
	
	if(parentNode->type == 0x05)
		newcell->fields.tableInternal.child_page = leftPage;
	else {
		newcell->fields.indexInternal.child_page = leftPage;
        newcell->fields.indexInternal.keyPk = middleCell->fields.indexInternal.keyPk;
    }

	err = chidb_Btree_insertCell(parentNode, parent_ncell, newcell);

    free(newcell);

    // Fixes right page of left and right nodes
	if(parentNode->type == 0x05)
	    leftNode->right_page = middleCell->fields.tableInternal.child_page;
	else
		leftNode->right_page = middleCell->fields.indexInternal.child_page;

    if(parentNode->right_page == nodeToSplit->right_page)
    	rightNode->right_page = parentNode->right_page;  
    else
		rightNode->right_page = nodeToSplit->right_page;
		
	// Write the new nodes
	err = chidb_Btree_writeNode(bt, parentNode);
	if(err != CHIDB_OK)
		return err;
	err = chidb_Btree_writeNode(bt, leftNode);
	if(err != CHIDB_OK)
		return err;
	err = chidb_Btree_writeNode(bt, rightNode);
	if(err != CHIDB_OK)
		return err;
    chidb_Btree_freeMemNode(bt, parentNode);
    chidb_Btree_freeMemNode(bt, leftNode);
    chidb_Btree_freeMemNode(bt, rightNode);

    return CHIDB_OK;
}














// White space just so I can center the above function in an editor
