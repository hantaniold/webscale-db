OBJS = main.o util.o btree.o pager.o record.o parser.o sql.yy.o sql.tab.o
DEPS = $(OBJS:.o=.d)
CC = gcc
CFLAGS = -I../../include -g3 -Wall -fpic -std=c99 -MMD -MP -D__key_t_defined -D_GNU_SOURCE
LDFLAGS = -shared
LIB = ../../libchidb.so

all: $(LIB)
	
$(LIB): $(OBJS)
	$(CC) $(LDFLAGS) -o$(LIB) $(OBJS)

%.d: %.c

clean:
	-rm -f $(OBJS) $(LIB) *.d *.yy.c *.yy.h *.tab.[hc]
	
-include $(DEPS)

sql.tab.c: sql.y
	yacc sql.y -b sql
			
sql.yy.c: sql.tab.c sql.lex
	lex --header-file=sql.yy.h -o sql.yy.c sql.lex 
