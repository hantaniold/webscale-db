/*****************************************************************************
 *
 *																 chidb
 *
 * This module provides the chidb API.
 *
 * For more details on what each function does, see the chidb Architecture
 * document, or the chidb.h header file.
 *
 * 2009, 2010 Borja Sotomayor - http://people.cs.uchicago.edu/~borja/
\*****************************************************************************/


#include <stdlib.h>
#include <chidb.h>
#include "btree.h"

/* This file currently provides only a very basic implementation
 * of chidb_open and chidb_close that are sufficient to test
 * the B Tree module. The rest of the functions are left
 * as an exercise to the student. chidb_open itself may need to be
 * modified if other chidb modules are implemented.
 */

int chidb_open(const char *file, chidb **db)
{
	*db = malloc(sizeof(chidb));
	if (*db == NULL)
		return CHIDB_ENOMEM;
	chidb_Btree_open(file, *db, &(*db)->bt);
	
	return CHIDB_OK;
}

int chidb_close(chidb *db)
{
	chidb_Btree_close(db->bt);
	free(db);
	return CHIDB_OK;
} 

int chidb_prepare(chidb *db, const char *sql, chidb_stmt **stmt)
{
	return CHIDB_OK;
}

int chidb_step(chidb_stmt *stmt)
{
	return CHIDB_OK;
}

int chidb_finalize(chidb_stmt *stmt)
{
	return CHIDB_OK;
}

int chidb_column_count(chidb_stmt *stmt)
{
	return 0;
}

int chidb_column_type(chidb_stmt *stmt, int col)
{
	return SQL_NOTVALID;
}

const char *chidb_column_name(chidb_stmt* stmt, int col)
{
	return "";
}

int chidb_column_int(chidb_stmt *stmt, int col)
{
	return 0;
}

const char *chidb_column_text(chidb_stmt *stmt, int col)
{
	return "";
}
