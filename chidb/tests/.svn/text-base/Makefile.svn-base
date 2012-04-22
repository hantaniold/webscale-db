OBJS = main.o tests-dbrecord.o tests-utils.o tests-pager.o tests-btree.o
DEPS = $(OBJS:.o=.d)
CC = gcc
CFLAGS = -I../include -I../src -Wall -std=c99 -MMD -MP -O2
#CFLAGS = -I../include -I../src -g3 -Wall -std=c99 -MMD -MP
BIN = tests
LDFLAGS = -L../ 
LDLIBS = -lchidb -lcunit

all: $(BIN)
	
$(BIN): $(OBJS)
	$(CC) $(LDFLAGS) $(LDLIBS) $(OBJS) -o$(BIN)
	
%.d: %.c

clean:
	-rm -f $(OBJS) $(LIB) *.d
	
-include $(DEPS)
