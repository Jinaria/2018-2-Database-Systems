#ifndef __BPT_H__
#define __BPT_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#ifdef WINDOWS
#define bool char
#define false 0
#define true 1
#endif

#define PAGE_SIZE 4096
#define LEAF_ORDER 32
#define INTERNAL_ORDER 249
#define MAX_LENGTH 120

typedef unsigned long long pagenum_t;

typedef struct leaf_record{
	int64_t key;
	char value[MAX_LENGTH];
}leaf_record;

typedef struct internal_record{
	int64_t key;
	pagenum_t page_offset;
}internal_record;

typedef struct page_t{
	pagenum_t page_offset;
	int is_leaf;
	int num_of_keys;
	pagenum_t right_page_offset;
	union{
		leaf_record leaf[LEAF_ORDER - 1];
		internal_record internal[INTERNAL_ORDER - 1];
	};
}page;

typedef struct header_page{
	pagenum_t free_page_offset;
	pagenum_t root_page_offset;
	int64_t num_of_pages;
}header_page;

extern header_page * header;
extern int leaf_order;
extern int internal_order;
extern int fd;

//Utility and Find
int height();
int path_to_root(pagenum_t child);
pagenum_t find_leaf(int64_t key);
char * find(int64_t key);
int cut(int length);
void print_tree();

// File I/O

pagenum_t file_alloc_page();
void file_free_page(pagenum_t pagenum);
pagenum_t increase_free_page();
void file_read_page(pagenum_t pagenum, page ** dest);
void file_read_header_page();
void file_write_page(pagenum_t pagenum, const page * src);
void file_write_header_page();

int open_db(char * pathname);

// Insertion

pagenum_t get_left_index(pagenum_t parent_num, pagenum_t left_num);
int insert_into_leaf(pagenum_t leaf, int64_t key, char * value);
int insert_into_leaf_after_splitting(pagenum_t leaf_num, int64_t key, char * value);
int insert_into_page(pagenum_t parent_num, int left_index, int64_t key, pagenum_t right);
int insert_into_page_after_splitting(pagenum_t old_page_num, int left_index, int64_t key, pagenum_t right_num);
int insert_into_parent(pagenum_t left_num, int64_t key, pagenum_t right_num);
int insert_into_new_root(pagenum_t left_num, int64_t key, pagenum_t right_num);
int start_new_tree(int64_t key, char * value);
int insert(int64_t key, char * value);

// Deletion

int get_neighbor_index(pagenum_t p);
int adjust_root();
int coalesce_pages(pagenum_t pn, pagenum_t neighbor_num, int neighbor_index, int64_t k_prime);
int delete_entry(pagenum_t p, int64_t key);
int delete(int64_t key);

#endif