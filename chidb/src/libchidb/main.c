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

    //Look at the root node? Traverse children and parse all
    //schema information into something using chidb_DBRecord_getString
    //text, text, text, integer, text
    //item type, table name, associated name, root page, sql statement
    //uh okay
    //Use chidb_DBRecord_create 
	return CHIDB_OK;
}

int chidb_load_schema(chidb * db) {
    DBRecord * dbr;
    BTreeNode * btn;
    BTreeCell * cell = calloc(1,sizeof(BTreeCell));
    chidb_Btree_getNodeByPage(db->bt, 1, &btn);
    int schema_size = 0;
    int schema_row_index = 0;
    for (int i = 0; i < btn->n_cells; i++) {
        printf("Getting schema %d\n",i);
        chidb_Btree_getCell(btn, i, cell);
        printf("Cell type: %x\n",cell->type);
        if (cell->type == PGTYPE_TABLE_LEAF) {
            schema_size++;
            //Maybe check retval of realloc
            db->bt->schema_table = realloc(db->bt->schema_table,schema_size*sizeof(SchemaTableRow *));
            db->bt->schema_table[schema_row_index] = calloc(1,sizeof(SchemaTableRow));

            chidb_DBRecord_unpack(&dbr,cell->fields.tableLeaf.data);
            chidb_DBRecord_getString(dbr,0,&db->bt->schema_table[schema_row_index]->item_type);
            chidb_DBRecord_getString(dbr,1,&db->bt->schema_table[schema_row_index]->item_name);
            chidb_DBRecord_getString(dbr,2,&db->bt->schema_table[schema_row_index]->assoc_table_name);
            chidb_DBRecord_getInt32(dbr,3,&db->bt->schema_table[schema_row_index]->root_page);
            chidb_DBRecord_getString(dbr,4,&db->bt->schema_table[schema_row_index]->sql);

            
            printf("%s\n%s\n%s\n%d\n%s\n",db->bt->schema_table[schema_row_index]->item_type, db->bt->schema_table[schema_row_index]->item_name, db->bt->schema_table[schema_row_index]->assoc_table_name, db->bt->schema_table[schema_row_index]->root_page,db->bt->schema_table[schema_row_index]->sql);
            schema_row_index++;
        }
    }
    db->bt->schema_table_size = schema_size;

    
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
    int err;

    // Call the SQL parser
    SQLStatement *sql_stmt;
    err = chidb_parser(sql, &sql_stmt);
    if(err == CHIDB_EINVALIDSQL)
        return CHIDB_EINVALIDSQL;

    // Check that the query is valid against our schema table
    switch(sql_stmt->type) {
        case STMT_SELECT:
        {
            // Check that all table names are valid
            SchemaTableRow *schema_row = NULL;
            for(int i = 0; i < sql_stmt->query.select.from_ntables; i++) {
                for(int j = 0; i < db->bt->schema_table_size; j++) {
                    if(!strcmp(sql_stmt->query.select.from_tables[i], db->bt->schema_table[j]->item_name)) {
                        schema_row = db->bt->schema_table[j];
                        break;
                    }
                }
                if(schema_row)
                    break;
            }
            if(!schema_row)
                return CHIDB_EINVALIDSQL;

            // Check that all column names are valid
            SQLStatement *create_table_stmt;
            chidb_parser(schema_row->sql, &create_table_stmt);

            for(int i = 0; i < sql_stmt->query.select.select_ncols; i++) {
                int match = 0;
                for(int j = 0; j < create_table_stmt->query.createTable.ncols; j++) {
                    if(!strcmp(sql_stmt->query.select.select_cols[i].name, create_table_stmt->query.createTable.cols[j].name)) {
                        match = 1;
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
            free(create_table_stmt);

            break;
        }
        case STMT_INSERT:
        {
            // Check that all table names are valid
            SchemaTableRow *schema_row = NULL;
            for(int i = 0; i < db->bt->schema_table_size; i++) {
                if(!strcmp(sql_stmt->query.insert.table, db->bt->schema_table[i]->item_name)) {
                    schema_row = db->bt->schema_table[i];
                    break;
                }
            }
            if(!schema_row)
                return CHIDB_EINVALIDSQL;

            // Check that the right number of values are being inserted
            SQLStatement *create_table_stmt;
            chidb_parser(schema_row->sql, &create_table_stmt);
            if(sql_stmt->query.insert.nvalues != create_table_stmt->query.createTable.ncols)
                return CHIDB_EINVALIDSQL;
            
            // Check value types
            for(int i = 0; i < sql_stmt->query.insert.nvalues; i++) {
                if(sql_stmt->query.insert.values[i].type != create_table_stmt->query.createTable.cols[i].type)
                    return CHIDB_EINVALIDSQL;
            }
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
