#ifndef __BPT_H__
#define __BPT_H__

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <sstream>
#include <vector>
#include <iostream>
#include <utility>
#ifdef WINDOWS
#endif

using namespace std;

#define PAGE_SIZE 4096
#define LEAF_ORDER 32
#define INTERNAL_ORDER 249
#define RECORD_SIZE 128
#define MAX_LENGTH 120
#define VALUE_SIZE 15
#define TABLE_LIMIT 10
#define BUFFER_SIZE 160

typedef unsigned long long pagenum_t;

typedef struct leaf_record{
	int64_t key;
	int64_t value[15];
}leaf_record;

typedef struct internal_record{
	int64_t key;
	pagenum_t page_offset;
}internal_record;

typedef struct page_header{
	pagenum_t page_offset;
	bool is_leaf;
	int num_of_keys;
	char reserve[128 - 16];
}page_header;

typedef struct internal_page{
	struct{
		pagenum_t page_offset;
		bool is_leaf;
		int num_of_keys;
		char reserve[128 - 16];
	};
	pagenum_t left_page_offset;
	internal_record internal[INTERNAL_ORDER - 1];
}internal_page;

typedef struct leaf_page{
	struct{
		pagenum_t page_offset;
		bool is_leaf;
		int num_of_keys;
		char reserve[128 - 16];
	};
	pagenum_t right_page_offset;
	leaf_record leaf[LEAF_ORDER - 1];
}leaf_page;

typedef struct header_page{
	pagenum_t free_page_offset;
	pagenum_t root_page_offset;
	int64_t num_of_pages;
	int64_t num_of_columns;
	char reserve[4064];
}header_page;

typedef struct page_t{
	union{
		struct{
			pagenum_t page_offset;
			int is_leaf;
			int num_of_keys;
			char reserve[120 - 16];
			union{
				struct{//internal
					pagenum_t left_page_offset;
					internal_record internal[INTERNAL_ORDER - 1];
				};
				struct{//leaf
					pagenum_t right_page_offset;
					leaf_record leaf[LEAF_ORDER - 1];
				};
			};
		};
		struct{//header
			pagenum_t free_page_offset;
			pagenum_t root_page_offset;
			int64_t num_of_pages;
			int64_t num_of_columns;
			char header_reserve[4064];			
		};
		// internal_page internal;
		// leaf_page leaf;
		// header_page header;
	};
}page;

typedef struct buffer{
	page * frame;
	int table_id;
	pagenum_t page_num;
	bool is_dirty;
	int pin_count;
	int next_index;
}buffer;

typedef struct buffer_manager{
	int capacity;
	int buffer_num;
	int lru;
	int mru;
	buffer * bf;
}buffer_manager;

typedef struct query{
	int t1;
	int c1;
	int t2;
	int c2;
}query_t;

typedef struct table{
	vector<int64_t *> record;
	int index_size;
}table;

extern buffer_manager bm;
extern int leaf_order;
extern int internal_order;
extern int fd[TABLE_LIMIT + 1];
extern int table_count;
extern int next_table_num;

//Utility and Find
//int path_to_root(pagenum_t child);
void print_tree(int table_id);
buffer * find_leaf(int table_id, int64_t key);
int64_t * find(int table_id, int64_t key);
int cut(int length);
//void print_tree();

// Buffer
int init_db(int num_buf);
int shutdown_db();
buffer * get_buffer(int table_id, pagenum_t pnum);


// File I/O

pagenum_t file_alloc_page(int table_id);
void file_free_page(buffer * buf);
pagenum_t increase_free_page(int table_id);
void file_read_page(int table_id, pagenum_t pagenum, page ** dest);
//void file_read_header_page();
void file_write_page(int table_id, pagenum_t pagenum, const page * src);
// void file_write_header_page();

int open_table(char * pathname, int num_column);
int close_table(int table_id);
// Insertion

pagenum_t get_left_index(buffer * parent, buffer * left);
int start_new_tree(buffer * header_buf, int64_t key, int64_t * value);
int insert_into_leaf(buffer * leaf, int64_t key, int64_t * value);
int insert_into_leaf_after_splitting(int table_id, buffer * leaf, int64_t key, int64_t * value);
int insert_into_parent(int table_id, buffer * left, int64_t key, buffer * right);
int insert_into_new_root(int table_id, buffer * left, int64_t key, buffer * right);
int insert_into_page(buffer * parent, int left_index, int64_t key, buffer * right);
int insert_into_page_after_splitting(buffer * old_page_num, int left_index, int64_t key, buffer * right);
int insert(int table_id, int64_t key, int64_t * value);

// Deletion

int get_neighbor_index(buffer * buf);
void remove_entry_from_page(buffer * buf, int64_t key);
int adjust_root(buffer * buf);
int coalesce_pages(buffer * buf, pagenum_t neighbor_num, int neighbor_index, int64_t k_prime);
int delete_entry(int table_id, buffer * buf, int64_t key);
int erase(int table_id, int64_t key);

// Join 
void swap(int * a, int * b);
int parse(string query);
query_t parse_query(string q);
vector<query_t> sort_query(vector<query_t> v);
void read_table(int table_id);
void join_one_query(query_t q);
int64_t join(const char * query);

#endif