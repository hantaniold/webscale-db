/*****************************************************************************
 *
 *																 chidb
 *
 * This is the header for the chidb 'internal' API.
 *
 * 2009, 2010 Borja Sotomayor - http://people.cs.uchicago.edu/~borja/
 * Some modifications by CMSC 23500 class of Spring 2009
\*****************************************************************************/

#ifndef CHIDBINT_H_
#define CHIDBINT_H_

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <assert.h>

// Public codes (specified in Architecture document)
#define CHIDB_OK (0)
#define CHIDB_EINVALIDSQL (1)
#define CHIDB_ENOMEM (2)
#define CHIDB_ECANTOPEN (3)
#define CHIDB_ECORRUPT (4)
#define CHIDB_ECONSTRAINT (5)
#define CHIDB_EMISMATCH (6)
#define CHIDB_EIO (7)
#define CHIDB_EMISUSE (8)

#define CHIDB_ROW (100)
#define CHIDB_DONE (101)

// Private codes (shouldn't be used by API users)
#define CHIDB_NOHEADER (1)
#define CHIDB_EFULLDB (3)
#define CHIDB_EPAGENO (4)      
#define CHIDB_ECELLNO (5)      
#define CHIDB_ECORRUPTHEADER (6)
#define CHIDB_ENOTFOUND (9)
#define CHIDB_EDUPLICATE (8)


#define DEFAULT_PAGE_SIZE (1024)

#define SQL_NOTVALID (-1)
#define SQL_NULL (0)
#define SQL_INTEGER_1BYTE (1)
#define SQL_INTEGER_2BYTE (2)
#define SQL_INTEGER_4BYTE (4)
#define SQL_TEXT (13)

typedef uint16_t ncell_t;
typedef uint32_t npage_t;
typedef uint32_t key_t;

/* Forward declaration */
typedef struct BTree BTree;

/* A chidb database is initially only a BTree.
 * This presuposes that only the btree.c module has been implemented.
 * If other parts of the chidb Architecture are implemented, the
 * chidb struct may have to be modified.
 */
struct chidb
{
	BTree   *bt;
};
typedef struct chidb chidb;


/* Error reporting macros. If DEBUG is #defined, messages will
 * be printed to stderr. Otherwise, they are ignored. */
#ifdef DEBUG
#define TRACE(msg)             fprintf(stderr, "[DEBUG %10s] %s\n", __FILE__, msg);
#define TRACEF(format, ...)    fprintf(stderr, "[DEBUG %10s] " format "\n", __FILE__,  __VA_ARGS__);
#define VTRACE(msg)            fprintf(stderr, "[DEBUG %10s]     %s\n", __FILE__, msg);
#define VTRACEF(format, ...)   fprintf(stderr, "[DEBUG %10s]     " format "\n", __FILE__,  __VA_ARGS__);
#else
#define TRACE(msg)        
#define TRACEF(format, ...) 
#define VTRACE(msg)            
#define VTRACEF(format, ...)     
#endif

/* Error reporting macros that terminate the program
 * (use for fatal errors) */
#define FATALF(fmt, ...) { \
	fprintf(stderr, fmt "\n", __VA_ARGS__); \
	fflush(stdout); \
	fflush(stderr); \
	exit(-1); \
} 

#define FATAL(str) { \
	fprintf(stderr, str "\n"); \
	fflush(stdout); \
	fflush(stderr); \
	exit(-1); \
} 

#endif /*CHIDBINT_H_*/
