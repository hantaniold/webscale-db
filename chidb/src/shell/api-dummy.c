/*****************************************************************************
 *
 *																 chidb
 *
 * This module provides a dummy chidb API that can be used to test
 * the chidb shell.
 *
 * If the chidb shell is linked with this dummy API, any SQL statement
 * run through the shell will parse the SQL statement and will always return
 * five identical rows. The rows have four columns:
 * four columns:
 *
 * - rowid: A row identifier (just for the purposes of returning an
 *          integer value)
 * - nullvalue: A null value (just for the purposes of returning one)
 * - providedsql: The SQL statement entered in the shell (and provided
 *                to chidb_prepare)
 * - parsedsql: The SQL statement as parsed by the chidb parser. Useful
 *              to check if the parser is working.
 *
 * 2009, 2010 Borja Sotomayor - http://people.cs.uchicago.edu/~borja/
\*****************************************************************************/

#include <chidb.h>
#include <stdlib.h>
#include "../libchidb/parser.h"

struct chidb_stmt
{
	int nrows;
	const char *sql;
	SQLStatement *stmt;
};

void trim(char *s)
{
	while(*s++) *s = (*s == '\n' || *s == '\t')?' ':*s;
}

int chidb_prepare(chidb *db, const char *sql, chidb_stmt **stmt)
{
	SQLStatement *sql_stmt;
	int rc;
	
	rc = chidb_parser(sql, &sql_stmt);
	trim(sql);
	
	if (rc != CHIDB_OK)
		return rc;
	
	*stmt = malloc(sizeof(chidb_stmt));
	
	(*stmt)->nrows = 5;
	(*stmt)->sql = sql;
	(*stmt)->stmt = sql_stmt;	
	
	return CHIDB_OK;
}

int chidb_step(chidb_stmt *stmt)
{
	if(stmt->nrows--)
		return CHIDB_ROW;
	else
		return CHIDB_OK;
	
	return CHIDB_OK;
}

int chidb_finalize(chidb_stmt *stmt)
{
	chidb_parser_SQLStatement_destroy(stmt->stmt);
	free(stmt);	
	
	return CHIDB_OK;
}

int chidb_column_count(chidb_stmt *stmt)
{
	return 4;
}

int chidb_column_type(chidb_stmt *stmt, int col)
{
	switch(col)
	{
		case 0: return SQL_INTEGER_4BYTE;
		case 1: return SQL_NULL;
		case 2: return SQL_TEXT;
		case 3: return SQL_TEXT;
	};
}

const char *chidb_column_name(chidb_stmt* stmt, int col)
{
	switch(col)
	{
		case 0: return "rowid";
		case 1: return "nullvalue";
		case 2: return "providedsql";
		case 3: return "parsedsql";
	};
}

int chidb_column_int(chidb_stmt *stmt, int col)
{
	if (col == 0)
		return stmt->nrows;
	else
		return -1;
}

const char *chidb_column_text(chidb_stmt *stmt, int col)
{
	char *s;
	switch(col)
	{
		case 2: return stmt->sql;
		case 3: 
			s = chidb_parser_StatementToString(stmt->stmt);
			trim(s);
			return s;
	};
}
