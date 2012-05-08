/*****************************************************************************
 *
 *																 chidb
 *
 * This module provides the chidb API.
 *
 * For more details on what each function does, see the chidb Architecture
 * document, or the chidb.h header file
 *
 * 2009, 2010 Borja Sotomayor - http://people.cs.uchicago.edu/~borja/
\*****************************************************************************/


#include <stdlib.h>
#include <chidb.h>
#include <string.h>
#include "btree.h"
#include "dbm.h"
#include "parser.h"
#include "record.h"

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
	
    chidb_load_schema(*db);
    chidb_print_schema(*db);

	return CHIDB_OK;
}

void chidb_print_schema(chidb *  db) {
    int schema_row_index = 0;
    printf("=== Printing Schema ===\n");
    for (;schema_row_index < db->bt->schema_table_size;  schema_row_index++) {

       printf("Entry %d\n",schema_row_index);
       printf("%s|%s|%s|%d|%s|\n",db->bt->schema_table[schema_row_index]->item_type, db->bt->schema_table[schema_row_index]->item_name, db->bt->schema_table[schema_row_index]->assoc_table_name, db->bt->schema_table[schema_row_index]->root_page,db->bt->schema_table[schema_row_index]->sql);
       return;
    }
}   
int chidb_load_schema(chidb * db) {
    DBRecord * dbr;
    BTreeNode * btn;
    BTreeCell * cell = calloc(1,sizeof(BTreeCell));
    chidb_Btree_getNodeByPage(db->bt, 1, &btn);
    int schema_size = 0;
    int schema_row_index = 0;
    for (int i = 0; i < btn->n_cells; i++) {
        chidb_Btree_getCell(btn, i, cell);
        if (cell->type == PGTYPE_TABLE_LEAF) {
            schema_size++;
            db->bt->schema_table = realloc(db->bt->schema_table,schema_size*sizeof(SchemaTableRow *));
            db->bt->schema_table[schema_row_index] = calloc(1,sizeof(SchemaTableRow));

            chidb_DBRecord_unpack(&dbr,cell->fields.tableLeaf.data);
            chidb_DBRecord_getString(dbr,0,&db->bt->schema_table[schema_row_index]->item_type);
            chidb_DBRecord_getString(dbr,1,&db->bt->schema_table[schema_row_index]->item_name);
            chidb_DBRecord_getString(dbr,2,&db->bt->schema_table[schema_row_index]->assoc_table_name);
            chidb_DBRecord_getInt32(dbr,3,&db->bt->schema_table[schema_row_index]->root_page);
            chidb_DBRecord_getString(dbr,4,&db->bt->schema_table[schema_row_index]->sql);

            
            schema_row_index++;
        }
    }
    db->bt->schema_table_size = schema_size;

    
    return CHIDB_OK;
}

int chidb_close(chidb *db)
{
	chidb_Btree_close(db->bt);

    for (int i = 0; i < db->bt->schema_table_size; i++) {
        free(db->bt->schema_table[i]);
    }
    free(db->bt->schema_table);
	free(db);
	return CHIDB_OK;
} 

int chidb_prepare(chidb *db, const char *sql, chidb_stmt **stmt)
{
    int err;

    // Call the SQL parser
    SQLStatement *sql_stmt;
    err = chidb_parser(sql, &sql_stmt);
    if(err == CHIDB_EINVALIDSQL)
        return CHIDB_EINVALIDSQL;

    // Check that the query is valid against our schema table
    int root_page;
    int ncols;
    SchemaTableRow *schema_row = NULL;
    SQLStatement *create_table_stmt;
    switch(sql_stmt->type) {
        case STMT_SELECT:
        {
            // Check that all table names are valid
            for(int i = 0; i < sql_stmt->query.select.from_ntables; i++) {
                for(int j = 0; i < db->bt->schema_table_size; j++) {
                    if(!strcmp(sql_stmt->query.select.from_tables[i], db->bt->schema_table[j]->item_name)) {
                        schema_row = db->bt->schema_table[j];
                        root_page = schema_row->root_page;
                        break;
                    }
                }
                if(schema_row)
                    break;
            }
            if(!schema_row)
                return CHIDB_EINVALIDSQL;

            // Check that all column names are valid
            chidb_parser(schema_row->sql, &create_table_stmt);

            for(int i = 0; i < sql_stmt->query.select.select_ncols; i++) {
                int match = 0;
                for(int j = 0; j < create_table_stmt->query.createTable.ncols; j++) {
                    if(!strcmp(sql_stmt->query.select.select_cols[i].name, create_table_stmt->query.createTable.cols[j].name)) {
                        match = 1;
                        ncols = create_table_stmt->query.createTable.ncols;
                        break;
                    }
                }
                if(!match)
                    return CHIDB_EINVALIDSQL;
            }

            // Check that all column names in the WHERE clause are valid
            int match = 0;
            uint8_t type = 0;
            for(int i = 0; i < sql_stmt->query.select.where_nconds; i++) {
                for(int j = 0; j < create_table_stmt->query.createTable.ncols; j++) {
                    if(!strcmp(sql_stmt->query.select.where_conds[i].op1.name, create_table_stmt->query.createTable.cols[j].name)) {
                        uint8_t op2t = sql_stmt->query.select.where_conds[i].op2Type;
                        if (op2t != OP2_COL && op2t == create_table_stmt->query.createTable.cols[j].type)
                            match = 1;
                        else {
                            match = 1;
                            type = create_table_stmt->query.createTable.cols[j].type;
                        }
                    }
                }
                if(!match)
                    return CHIDB_EINVALIDSQL;
                if(sql_stmt->query.select.where_conds[i].op2Type == OP2_COL) {
                    match = 0;

                    for(int i = 0; i < sql_stmt->query.select.where_nconds; i++) {
                        for(int j = 0; j < create_table_stmt->query.createTable.ncols; j++) {
                            if(!strcmp(sql_stmt->query.select.where_conds[i].op2.col.name, create_table_stmt->query.createTable.cols[j].name) && type == create_table_stmt->query.createTable.cols[j].type) {
                                match = 1;
                            }
                        }
                        if(!match)
                            return CHIDB_EINVALIDSQL;
                    }
                }
            }

            break;
        }
        case STMT_INSERT:
        {
            // Check that all table names are valid
            for(int i = 0; i < db->bt->schema_table_size; i++) {
                if(!strcmp(sql_stmt->query.insert.table, db->bt->schema_table[i]->item_name)) {
                    schema_row = db->bt->schema_table[i];
                    root_page = schema_row->root_page;
                    break;
                }
            }
            if(!schema_row)
                return CHIDB_EINVALIDSQL;

            // Check that the right number of values are being inserted
            chidb_parser(schema_row->sql, &create_table_stmt);
            if(sql_stmt->query.insert.nvalues != create_table_stmt->query.createTable.ncols)
                return CHIDB_EINVALIDSQL;

            ncols = create_table_stmt->query.createTable.ncols;
            
            // Check value types
            for(int i = 0; i < sql_stmt->query.insert.nvalues; i++) {
                if(sql_stmt->query.insert.values[i].type != create_table_stmt->query.createTable.cols[i].type)
                    return CHIDB_EINVALIDSQL;
            }
            break;
        }
    }

    // Compile the SQL statement into valid chidb statements
    switch(sql_stmt->type) {
        case STMT_SELECT:
        {
            // Basic outline:
            // 1) Store the page number
            // 2) Store any WHERE literal
            // 3) OpenRead
            // 4) Rewind
            // 5) Select columns (doing any needed comparisons after each column selection)
            //    5a) If all is well, ResultRow, then Next
            //    5b) If all is not well, jump to a Next
            // 6) Close
            // 7) Halt

            int numlines = 0;
            int rmax = 0;

            // Store the page number
            *stmt = malloc(sizeof(chidb_stmt));
            (*stmt)[numlines].instruction = DBM_INTEGER;    // Integer type
            (*stmt)[numlines].P1 = root_page;               // Store the root page
            (*stmt)[numlines].P2 = 0;                       // into register 0
            numlines++;

            // Store any literals
            if(sql_stmt->query.select.where_nconds > 0 && sql_stmt->query.select.where_conds[0].op2Type == OP2_INT) {
                *stmt = realloc(*stmt, (numlines + 1) * sizeof(chidb_stmt));
                (*stmt)[numlines].instruction = DBM_INTEGER;                                // Integer type
                (*stmt)[numlines].P1 = sql_stmt->query.select.where_conds[0].op2.integer;   // Store the integer
                (*stmt)[numlines].P2 = 1;                                                   // into register 1
                numlines++;
                rmax = 1;
            } else if(sql_stmt->query.select.where_nconds > 0 && sql_stmt->query.select.where_conds[0].op2Type == OP2_STR) {
                *stmt = realloc(*stmt, (numlines + 1) * sizeof(chidb_stmt));
                (*stmt)[numlines].instruction = DBM_STRING;                                             // String type
                (*stmt)[numlines].P1 = strlen(sql_stmt->query.select.where_conds[0].op2.string) + 1;    // Store the length
                (*stmt)[numlines].P2 = 1;                                                               // into register 1
                (*stmt)[numlines].P4 = (uint32_t)(sql_stmt->query.select.where_conds[0].op2.string);    // and keep a ptr
                numlines++;
                rmax = 1;
            }

            // Open the B-Tree
            *stmt = realloc(*stmt, (numlines + 1) * sizeof(chidb_stmt));
            (*stmt)[numlines].instruction = DBM_OPENREAD;  // Open a B-Tree
            (*stmt)[numlines].P1 = 0;                       // with cursor 0 (TODO: how should cursor numbers be assigned?)
            (*stmt)[numlines].P2 = 0;                       // on the page in register 0
            (*stmt)[numlines].P3 = ncols;                   // having ncols columns
            numlines++;

            // Rewind the B-Tree
            *stmt = realloc(*stmt, (numlines + 1) * sizeof(chidb_stmt));
            (*stmt)[numlines].instruction = DBM_REWIND;    // Rewind to the beginning of the B-Tree
            (*stmt)[numlines].P1 = 0;                       // using cursor 0 (TODO: as above)
            (*stmt)[numlines].P2 = -1;                      // and if the table is empty, jump to CLOSE (TODO)
            numlines++;

            // Select Columns
            for(int i = 0; i < sql_stmt->query.select.select_ncols; i++) {
                // Get the column number
                int colnum;
                for(int j = 0; j < ncols; j++) {
                    if(!strcpy(sql_stmt->query.select.select_cols[i].name, create_table_stmt->query.createTable.cols[j].name)) {
                        colnum = j;
                        break;
                    }
                }

                // Store the column value
                *stmt = realloc(*stmt, (numlines + 1) * sizeof(chidb_stmt));
                (*stmt)[numlines].instruction = DBM_COLUMN;    // Get a column value
                (*stmt)[numlines].P1 = 0;                       // using cursor 0 (TODO: as above)
                (*stmt)[numlines].P2 = colnum;                  // from column number colnum
                (*stmt)[numlines].P3 = ++rmax;                  // into a new register
                numlines++;
            }

            break;
        }
        case STMT_INSERT:
        {
            break;
        }
    }


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
