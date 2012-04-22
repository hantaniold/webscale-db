#ifndef UTIL_H_
#define UTIL_H_

#include "btree.h"
/*
** Read or write a two- and four-byte big-endian integer values.
* Based on SQLite code
*/
#define get2byte(x)   ((x)[0]<<8 | (x)[1])
#define put2byte(p,v) ((p)[0] = (uint8_t)((v)>>8), (p)[1] = (uint8_t)(v))

/* Return the distance in bytes between the pointers elm and hd */
#define OFFSET(hd, elm) ((uint8_t *)(&(elm))-(uint8_t *)(&hd))

uint32_t get4byte(const uint8_t *p);
void put4byte(unsigned char *p, uint32_t v);
int getVarint32(const uint8_t *p, uint32_t *v);
int putVarint32(uint8_t *p, uint32_t v);

int chidb_astrcat(char **dst, char *src);

typedef void (*fBTreeCellPrinter)(BTreeNode *, BTreeCell*);
int chidb_Btree_print(BTree *bt, npage_t nroot, fBTreeCellPrinter printer, bool verbose);
void chidb_BTree_recordPrinter(BTreeNode *btn, BTreeCell *btc);
void chidb_BTree_stringPrinter(BTreeNode *btn, BTreeCell *btc);

#endif /*UTIL_H_*/
