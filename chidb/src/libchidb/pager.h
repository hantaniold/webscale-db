#ifndef PAGER_H_
#define PAGER_H_

#include <stdio.h>
#include <chidbInt.h>

struct MemPage
{
	npage_t npage;
	uint8_t *data;
};
typedef struct MemPage MemPage;

struct Pager
{
	FILE *f;
	npage_t n_pages;
	uint16_t page_size;
};
typedef struct Pager Pager;

int chidb_Pager_open(Pager **pager, const char *filename);
int chidb_Pager_setPageSize(Pager *pager, uint16_t pagesize);
int chidb_Pager_readHeader(Pager *pager, uint8_t *header);
int chidb_Pager_allocatePage(Pager *pager, npage_t *npage);
int chidb_Pager_releaseMemPage(Pager *pager, MemPage *page);
int	chidb_Pager_readPage(Pager *pager, npage_t page_num, MemPage **page);
int chidb_Pager_writePage(Pager *pager, MemPage *page);
int chidb_Pager_getRealDBSize(Pager *pager, npage_t *npages);
int chidb_Pager_close(Pager *pager);

#endif /*PAGER_H_*/
