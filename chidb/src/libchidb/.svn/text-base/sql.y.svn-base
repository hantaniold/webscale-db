%{
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sql.yy.h"
#include "parser.h"
int yylex (void);
extern int yylineno;
void yyerror (char const *msg);

SQLStatement *__stmt;
%}

%defines

%union {
    int integer;
    char *string;
}

%token TK_SELECT TK_FROM TK_WHERE TK_STAR
%token TK_INSERT TK_INTO TK_VALUES
%token TK_CREATE TK_TABLE TK_BYTE TK_SMALLINT TK_INTEGER TK_TEXT TK_PRIMARY TK_KEY
%token TK_INDEX TK_ON
%token TK_EXPLAIN
%token TK_LPAREN TK_RPAREN TK_SEMICOLON TK_DOT TK_COMMA
%token TK_AND
%token TK_LTE TK_GTE TK_NE TK_GT TK_LT TK_EQ TK_IS TK_NOT
%token<integer> TK_INT
%token<string> TK_ID TK_STRING
%token TK_NULL


%% 

/*************/
/* Statement */
/*************/
statement: 
	explain_statement 
	| 
	sql_statement;

explain_statement:
	TK_EXPLAIN sql_statement;


/*****************/
/* SQL Statement */
/*****************/
sql_statement: 	
	select_statement TK_SEMICOLON 
	
	{
		#ifdef DEBUG
		TRACE("The parsed SELECT statement is:");
		chidb_parser_printSelect(__stmt);
		#endif
	}

	| 

	insert_statement TK_SEMICOLON
	
	{
		#ifdef DEBUG
		TRACE("The parsed INSERT statement is:");
		chidb_parser_printInsert(__stmt);
		#endif
	}
	 
	| 
	
	createtable_statement TK_SEMICOLON

	{
		#ifdef DEBUG
		TRACE("The parsed CREATE TABLE statement is:");
		chidb_parser_printCreateTable(__stmt);
		#endif
	}
	
	|
	
	createindex_statement TK_SEMICOLON
	 
	{
		#ifdef DEBUG
		TRACE("The parsed CREATE INDEX statement is:");
		chidb_parser_printCreateIndex(__stmt);
		#endif
	}
	;


/********************/
/* SELECT statement */
/********************/

select_statement: 

	TK_SELECT 
	
	{
		chidb_parser_initSelectStmt(__stmt);
	}
	
	select_clause TK_FROM from_clause where_clause;

select_clause: 
	TK_STAR 
	
	{
		__stmt->query.select.select_ncols = SELECT_ALL;	
	}
	
	| 
	sel_collist
	;

/* SELECT clause */
sel_collist: 
	sel_col sel_collist_r;

sel_collist_r: 
	TK_COMMA sel_col sel_collist_r 
	|
	/* Empty */ 
	;

sel_col: 
	TK_ID
	 
	{
		chidb_parser_addSelectColumn(__stmt, NULL, $1);
	}		
	
	| 
	
	TK_ID TK_DOT TK_ID
	
	{
		chidb_parser_addSelectColumn(__stmt, $1, $3);
	}		
	
	;


/* FROM clause */

from_clause: 
	table from_clause_r;

from_clause_r: 
	TK_COMMA table from_clause_r 
	|
	/* Empty */
	;

table:
	TK_ID

	{
		chidb_parser_addFromTable(__stmt, $1);
	}		
	
	;

/* WHERE clause */

where_clause: 
	TK_WHERE 
	
	{
		chidb_parser_newCondition(__stmt);
	}				
	
	cond where_clause_r 
	
	| 
	/* Empty */
	;

where_clause_r: 
	TK_AND 

	{
		chidb_parser_newCondition(__stmt);
	}				
	
	cond where_clause_r 
	
	|
	/* Empty */
	;

cond: 

	cond_op1_col cond_op TK_INT 
	
	{
		chidb_parser_setConditionOperand2Integer(__stmt, $3);
	}
	
	| 
	
	cond_op1_col cond_op TK_STRING 
	
	{
		chidb_parser_setConditionOperand2String(__stmt, $3);
	}

	| 
	
	cond_op1_col TK_IS TK_NULL
	
	{
		chidb_parser_setConditionOperator(__stmt, OP_ISNULL);
	}
	
	| 
	
	cond_op1_col TK_IS TK_NOT TK_NULL
	
	{
		chidb_parser_setConditionOperator(__stmt, OP_ISNOTNULL);
	}

	| 
	
	cond_op1_col cond_op cond_op2_col
	
	;

cond_op1_col:
	TK_ID
	 
	{
		chidb_parser_setConditionOperand1(__stmt, NULL, $1);
	}		
	
	| 
	
	TK_ID TK_DOT TK_ID
	
	{
		chidb_parser_setConditionOperand1(__stmt, $1, $3);
	}		
	
	;
	
cond_op2_col:
	TK_ID
	 
	{
		chidb_parser_setConditionOperand2Column(__stmt, NULL, $1);
	}		
	
	| 
	
	TK_ID TK_DOT TK_ID
	
	{
		chidb_parser_setConditionOperand2Column(__stmt, $1, $3);
	}		
	
	;

cond_op: 

	TK_LTE 

	{
		chidb_parser_setConditionOperator(__stmt, OP_LTE);
	}		
	
	| 
	
	TK_GTE 
	
	{
		chidb_parser_setConditionOperator(__stmt, OP_GTE);
	}		
	
	| 
	
	TK_NE 
	
	{
		chidb_parser_setConditionOperator(__stmt, OP_NE);
	}		
	
	| 
	
	TK_GT 
	
	{
		chidb_parser_setConditionOperator(__stmt, OP_GT);
	}		
	
	| 
	
	TK_LT 
	
	{
		chidb_parser_setConditionOperator(__stmt, OP_LT);
	}		
	
	| 
	
	TK_EQ

	{
		chidb_parser_setConditionOperator(__stmt, OP_EQ);
	}
	
	;	
	


/********************/
/* INSERT statement */
/********************/

insert_statement: 
	TK_INSERT TK_INTO 
	
	{
		chidb_parser_initInsertStmt(__stmt);
	} 
	
	TK_ID TK_VALUES TK_LPAREN insert_val_list TK_RPAREN 
	
	{
		chidb_parser_setInsertTable(__stmt, $4);
	} 
	
	;

insert_val_list: 
	insert_val insert_val_list_r;

insert_val_list_r: 
	TK_COMMA insert_val insert_val_list_r 
	| 
	/* Empty */
	;

insert_val: 
	TK_INT 
	
	{
		chidb_parser_addInsertIntValue(__stmt, $1);
	} 

	| 
	
	TK_STRING
	{
		chidb_parser_addInsertStrValue(__stmt, $1);
	} 

	| 
	
	TK_NULL
	{
		chidb_parser_addInsertNullValue(__stmt);
	} 


/**************************/
/* CREATE TABLE statement */
/**************************/

createtable_statement:
	TK_CREATE TK_TABLE
	
	{
		chidb_parser_initCreateTableStmt(__stmt);
	} 		
	
	TK_ID TK_LPAREN cr_collist TK_RPAREN

	{
		chidb_parser_setCreateTableName(__stmt, $4);
	} 

	;


cr_collist: 
	cr_col cr_collist_r;

cr_collist_r: 
	TK_COMMA cr_col cr_collist_r 
	| 
	/* Empty */
	;

cr_col: 
	TK_ID TK_BYTE 
	
	{
		chidb_parser_addCreateTableColumn(__stmt, $1, SQL_INTEGER_1BYTE, 0);
	}
	
	| 
	
	TK_ID TK_SMALLINT
	
	{
		chidb_parser_addCreateTableColumn(__stmt, $1, SQL_INTEGER_2BYTE, 0);
	}
	
	| 
	
	TK_ID TK_INTEGER
	
	{
		chidb_parser_addCreateTableColumn(__stmt, $1, SQL_INTEGER_4BYTE, 0);
	}
	
	| 
	
	TK_ID TK_TEXT
	
	{
		chidb_parser_addCreateTableColumn(__stmt, $1, SQL_TEXT, 0);
	}

	| 
	
	TK_ID TK_INTEGER TK_PRIMARY TK_KEY
	
	{
		chidb_parser_addCreateTableColumn(__stmt, $1, SQL_INTEGER_4BYTE, 1);
	}
	
	;

createindex_statement:

	TK_CREATE TK_INDEX TK_ID TK_ON TK_ID TK_LPAREN TK_ID TK_RPAREN
	
	{
		chidb_parser_initCreateIndexStmt(__stmt, $3, $5, $7);
	} 		
	


%%

void yyerror(char const *str)
{
        fprintf(stderr,"Error [line %d]: %s\n", yylineno, str);
}

int chidb_parser(const char *sql, SQLStatement **stmt)
{
	int rc;
	
	__stmt = malloc(sizeof(SQLStatement));
	
	TRACEF("The SQL statement to parse is: %s", sql);
	
	YY_BUFFER_STATE my_string_buffer = yy_scan_string (sql);
	rc = yyparse();
	yy_delete_buffer (my_string_buffer);
	
	if (rc == 0)
	{
		*stmt = __stmt;
		return CHIDB_OK;
	}
	else
	{
		free(__stmt);
		return CHIDB_EINVALIDSQL;
	}
}
