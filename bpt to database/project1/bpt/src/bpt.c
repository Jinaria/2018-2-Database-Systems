#include "bpt.h"

header_page * header = NULL;
int leaf_order = LEAF_ORDER;
int internal_order = INTERNAL_ORDER;
pagenum_t queue[100];
int start = 0, end = -1;

int fd = -1;

int height(){
    int h = 0;

    page * root;
    file_read_page(header->root_page_offset, &root);
    while(!root->is_leaf){
        file_read_page(root->internal[0].page_offset, &root);
        h++;
    }
    free(root);
    return h;
}

int path_to_root(pagenum_t child){
    int length = 0;

    page * c;
    file_read_page(child, &c);
    while(c->page_offset != header->root_page_offset){
        file_read_page(c->page_offset, &c);
        length++;
    }
    length++;
    free(c);
    return length;
}

pagenum_t find_leaf(int64_t key){
    int i = 0;
    page * c;
    pagenum_t p = header->root_page_offset;
    
    file_read_page(p, &c);
    if(c == NULL)
        return 0;
    while(!c->is_leaf){
        i = -1;
        if(key < c->internal[0].key){
            p = c->right_page_offset;
            file_read_page(p, &c);
            continue;
        }
        for(i = c->num_of_keys - 1; i > -1; i--){
            if(key >= c->internal[i].key)
                break;
        }
        p = c->internal[i].page_offset;
        file_read_page(p, &c);
    }
    free(c);
    return p;
}

char * find(int64_t key){
    int i = 0;
    page * c;
    pagenum_t p = find_leaf(key);
    char * value = (char *)malloc(MAX_LENGTH);
    file_read_page(p, &c);

    if(p == 0) return NULL;
    for(i = 0; i < c->num_of_keys; i++){
        if(c->leaf[i].key == key) break;
    }
    if(i == c->num_of_keys){
        return NULL;
    }
    strcpy(value, c->leaf[i].value);
    free(c);
    return value;
    
}

int cut(int length){
    if(length % 2 == 0)
        return length / 2;
    else
        return length / 2 + 1;
}

void print_tree(){
    page * p, * parent;
    pagenum_t pn;
    int i = 0;
    int rank = 0;
    int new_rank = 0;

    if(header == NULL){
        printf("DB is not opened\n");
        return;
    }
    if(header->root_page_offset == 0){
        printf("Empty tree\n");
        return;
    }
    queue[++end] = header->root_page_offset;
    while(end >= start){
        pn = queue[start++];
        file_read_page(pn, &p);
        file_read_page(p->page_offset, &parent);
        if(p->page_offset != 0 && pn == parent->right_page_offset){
            new_rank = path_to_root(pn);
            if(new_rank != rank){
                rank = new_rank;
                printf("\n");
            }
        }
        for(i = 0; i < p->num_of_keys; i++){
            if(!p->is_leaf)
                printf("%ld ", p->internal[i].key);
            else
                printf("%ld ", p->leaf[i].key);
        }
        if(!p->is_leaf){
            queue[++end] = p->right_page_offset;
            for(i = 0; i < p->num_of_keys; i++)
                queue[++end] = p->internal[i].page_offset;
        }
        printf("| ");
    }
    printf("\n");
}


int open_db(char * pathname){
    int temp = 0;
    fd = open(pathname, O_RDWR | O_CREAT | O_EXCL | O_SYNC, 0777);
    header = (header_page*)calloc(1, PAGE_SIZE);

    if(fd > 0){
        header->num_of_pages = 1;
        temp = pwrite(fd, header, PAGE_SIZE, SEEK_SET);
        
        if(temp < PAGE_SIZE){
            printf("Failed to write header_page\n");
            exit(EXIT_FAILURE);
        }
        return 0;
    }
    fd = open(pathname, O_RDWR | O_SYNC);
    if(fd > 0){
        temp = pread(fd, header, PAGE_SIZE, SEEK_SET);
        if (temp < PAGE_SIZE) {
            printf("Failed to read header_page\n");
            exit(EXIT_FAILURE);
        }
        return 0;
    }
    printf("Failed to open\n");
    return -1;
}

pagenum_t file_alloc_page(){
    pagenum_t pn;
    page * p;
    if(header->free_page_offset == 0){
        return increase_free_page();
    }
    pn = header->free_page_offset;
    file_read_page(pn, &p);
    header->free_page_offset = p->page_offset;
    file_write_header_page();
    free(p);
    return pn;
}

void file_free_page(pagenum_t pagenum){
    page * p;

    file_read_page(pagenum, &p);
    p->page_offset = header->free_page_offset;
    p->is_leaf = 1;
    p->right_page_offset = 0;
    p->num_of_keys = 0;
    header->free_page_offset = pagenum;
    file_write_page(pagenum, p);
    file_write_header_page();
    free(p);
}

pagenum_t increase_free_page(){
    int i, temp;
    pagenum_t pn = header->num_of_pages;
    page * p = (page*)calloc(1, PAGE_SIZE);
    p->page_offset = header->free_page_offset;
    p->num_of_keys = 0;
    p->is_leaf = 1;
    header->free_page_offset = pn;
    header->num_of_pages++;
    file_write_page(pn, p);
    file_write_header_page();
    free(p);
    return pn;
}

void file_read_page(pagenum_t pagenum, page ** dest){
    int temp;
    *dest = (page * )calloc(1, PAGE_SIZE);
    temp = pread(fd, *dest, PAGE_SIZE, PAGE_SIZE * pagenum);
    if(temp < PAGE_SIZE){
        printf("Failed to read from file_read_page\n");
        exit(EXIT_FAILURE);
    }
}

void file_read_header_page(){
    int temp;
    temp = pread(fd, header, PAGE_SIZE, 0);
    if(temp < PAGE_SIZE){
        printf("Failed to read header");
        exit(EXIT_FAILURE);
    }
}

void file_write_page(pagenum_t pagenum, const page * src){
    
    int temp;
    temp = pwrite(fd, src, PAGE_SIZE, pagenum * PAGE_SIZE);
    if(temp < PAGE_SIZE){
        printf("Failed to write from file_write_page\n");
        exit(EXIT_FAILURE);
    }
}

void file_write_header_page(){
    
    int temp;
    temp = pwrite(fd, header, PAGE_SIZE, 0);
    if(temp < PAGE_SIZE){
        printf("Failed to write header\n");
        exit(EXIT_FAILURE);
    }   

}

//Insertion

pagenum_t get_left_index(pagenum_t parent_num, pagenum_t left_num){
    page * parent;
    int left_index = 0;
    file_read_page(parent_num, &parent);

    if(parent->right_page_offset == left_num)
        return -1;
    while(left_index <= parent->num_of_keys - 1 &&
            parent->internal[left_index].page_offset != left_num)
        left_index++;
    free(parent);
    return left_index;
}

int start_new_tree(int64_t key, char * value){
    int i;
    page * root;
    if(header->free_page_offset == 0){
        for(i = 0; i < 10; i++){
            increase_free_page();
        }   
    }
    
    header->root_page_offset = file_alloc_page();
    file_read_page(header->root_page_offset, &root);
    
    root->page_offset = 0;
    root->is_leaf = true;
    root->num_of_keys = 1;
    root->right_page_offset = 0;
    root->leaf[0].key = key;
    strcpy(root->leaf[0].value, value);
    file_write_header_page();
    file_write_page(header->root_page_offset, root);
    free(root);
    return 0;
}

int insert_into_leaf(pagenum_t leaf_num, int64_t key, char * value){
    int i, insertion_point = 0;
    page * leaf;
    file_read_page(leaf_num, &leaf);
    while(insertion_point < leaf->num_of_keys && leaf->leaf[insertion_point].key < key)
        insertion_point++;

    for(i = leaf->num_of_keys; i > insertion_point; i--){
        leaf->leaf[i] = leaf->leaf[i - 1];
    }
    leaf->leaf[insertion_point].key = key;
    strcpy(leaf->leaf[insertion_point].value, value);
    leaf->num_of_keys++;
    file_write_page(leaf_num, leaf);
    free(leaf);

    return 0;

}

int insert_into_leaf_after_splitting(pagenum_t leaf_num, int64_t key, char * value){
    page * leaf;
    page * new_leaf;
    pagenum_t new_leaf_num;
    int insertion_point, split, i, j;
    int64_t new_key;
    leaf_record temp[leaf_order];

    file_read_page(leaf_num, &leaf);
    new_leaf_num = file_alloc_page();
    file_read_page(new_leaf_num, &new_leaf);

    insertion_point = 0;
    while(insertion_point < leaf_order - 1 && leaf->leaf[insertion_point].key < key){
        insertion_point++;
    }

    for(i = 0, j = 0; i < leaf->num_of_keys; i++, j++){
        if(j == insertion_point) j++;
        temp[j] = leaf->leaf[i];
    }
    temp[insertion_point].key = key;
    strcpy(temp[insertion_point].value, value);

    leaf->num_of_keys = 0;

    split = cut(leaf_order);

    for(i = 0; i < split; i++){
        leaf->leaf[i] = temp[i];
        leaf->num_of_keys++;
    }
    for(i = split, j = 0; i < leaf_order; i++, j++){
        new_leaf->leaf[j] = temp[i];
        new_leaf->num_of_keys++;
    }
    new_leaf->right_page_offset = leaf->right_page_offset;
    leaf->right_page_offset = new_leaf_num;
    
    for(i = leaf->num_of_keys; i < leaf_order; i++){
        leaf->leaf[i].key = 0;
        strcpy(leaf->leaf[i].value, "");
    }
    for(i = new_leaf->num_of_keys; i < leaf_order; i++){
        new_leaf->leaf[i].key = 0;
        strcpy(new_leaf->leaf[i].value, "");
    }

    new_leaf->page_offset = leaf->page_offset;
    new_key = new_leaf->leaf[0].key;
    
    file_write_header_page();
    file_write_page(new_leaf_num, new_leaf);
    file_write_page(leaf_num, leaf);
    free(leaf);
    free(new_leaf);
    return insert_into_parent(leaf_num, new_key, new_leaf_num);
}

int insert_into_parent(pagenum_t left_num, int64_t key, pagenum_t right_num){
    int left_index;
    page * parent;
    page * left;

    file_read_page(left_num, &left);
    if(left->page_offset == 0)
        return insert_into_new_root(left_num, key, right_num);

    file_read_page(left->page_offset, &parent);

    left_index = get_left_index(left->page_offset, left_num);
    
    if(parent->num_of_keys < internal_order - 1)
        return insert_into_page(left->page_offset, left_index, key, right_num);

    return insert_into_page_after_splitting(left->page_offset, left_index, key, right_num);
}

int insert_into_new_root(pagenum_t left_num, int64_t key, pagenum_t right_num){
    pagenum_t root_num = file_alloc_page();
    page * root;
    page * left;
    page * right;
    file_read_page(root_num, &root);
    file_read_page(left_num, &left);
    file_read_page(right_num, &right);
    header->root_page_offset = root_num;
    root->page_offset = 0;
    root->is_leaf = 0;
    root->num_of_keys = 1;
    root->right_page_offset = left_num;
    root->internal[0].key = key;
    root->internal[0].page_offset = right_num;
    left->page_offset = root_num;
    right->page_offset = root_num;
    file_write_header_page();
    file_write_page(root_num, root);
    file_write_page(left_num, left);
    file_write_page(right_num, right);
    free(root);
    free(left);
    free(right);
    return 0;   
}

int insert_into_page(pagenum_t parent_num, int left_index, int64_t key, pagenum_t right){
    int i;
    page * parent;
    file_read_page(parent_num, &parent);
    for(i = parent->num_of_keys - 1; i > left_index; i--){
        parent->internal[i + 1] = parent->internal[i];
    }

    parent->internal[left_index + 1].key = key;
    parent->internal[left_index + 1].page_offset = right;
    parent->num_of_keys++;
    file_write_page(parent_num, parent);
    free(parent);
    return 0;
}

int insert_into_page_after_splitting(pagenum_t old_page_num, int left_index, int64_t key, pagenum_t right_num){
    int i, j, split;
    int64_t k_prime;
    internal_record temp[internal_order];
    pagenum_t first_page_offset, new_page_num;
    page * old_page, * new_page, * child;

    file_read_page(old_page_num, &old_page);

    first_page_offset = old_page->right_page_offset;
    for(i = 0, j = 0; i < old_page->num_of_keys; i++, j++){
        if(j == left_index + 1) j++;
        temp[j] = old_page->internal[i];
    }
    temp[left_index + 1].key = key;
    temp[left_index + 1].page_offset = right_num;
    

    split = cut(internal_order);
    new_page_num = file_alloc_page();
    file_read_page(new_page_num, &new_page);
    old_page->num_of_keys = 0;

    old_page->right_page_offset = first_page_offset;
    for(i = 0; i < split - 1; i++){
        old_page->internal[i] = temp[i];
        old_page->num_of_keys++;
    }
    new_page->num_of_keys = 0;
    new_page->right_page_offset = temp[split - 1].page_offset;
    new_page->is_leaf = 0;
    k_prime = temp[split - 1].key;
    for(i = split, j = 0; i < internal_order; i++, j++){
        new_page->internal[j] = temp[i];
        new_page->num_of_keys++;
    }
    new_page->page_offset = old_page->page_offset;
    file_read_page(new_page->right_page_offset, &child);
    child->page_offset = new_page_num;
    file_write_page(new_page->right_page_offset, child);
    for(i = 0; i < new_page->num_of_keys; i++){
        file_read_page(new_page->internal[i].page_offset, &child);
        child->page_offset = new_page_num;
        file_write_page(new_page->internal[i].page_offset, child);
    }
    file_write_page(old_page_num, old_page);
    file_write_page(new_page_num, new_page);

    free(old_page);
    free(new_page);
    free(child);

    return insert_into_parent(old_page_num, k_prime, new_page_num);
}

int insert(int64_t key, char * value){
    pagenum_t leaf_num;
    page * leaf;
    
    if(header == NULL){
        printf("DB is not opened\n");
        return -1;
    }
    if(header->root_page_offset == 0){
        return start_new_tree(key, value);
    }
    if(find(key) != NULL)
        return -1;
    leaf_num = find_leaf(key);
    file_read_page(leaf_num, &leaf);
    if(leaf->num_of_keys < leaf_order - 1){
        insert_into_leaf(leaf_num, key, value);
        free(leaf);
        return 0;
    }
    insert_into_leaf_after_splitting(leaf_num, key, value);
    free(leaf);
    return 0;
}

//Deletion

int get_neighbor_index(pagenum_t n){
    int i;
    page * p, * parent;
    file_read_page(n, &p);
    file_read_page(p->page_offset, &parent);
    if(parent->right_page_offset == n){
        free(p);
        free(parent);
        return -2;
    }
    for(i = 0; i < parent->num_of_keys; i++){
        if(parent->internal[i].page_offset == n){
            free(parent);
            free(p);
            return i - 1;
        }
    }
}

void remove_entry_from_page(pagenum_t pn, int64_t key){
    int i;
    page * p;
    file_read_page(pn, &p);
    i = 0;
    if(p->is_leaf){
        while(p->leaf[i].key != key)
            i++;
        for(++i; i < p->num_of_keys; i++)
            p->leaf[i - 1] = p->leaf[i];
    }
    else{

        while(p->internal[i].key != key)
            i++;
        for(++i; i < p->num_of_keys; i++)
            p->internal[i - 1] = p->internal[i];
    }   
    p->num_of_keys--;
    file_write_page(pn, p);
    free(p);
}

int adjust_root(pagenum_t pn){
    page * p, * child;
    file_read_page(pn, &p);
    
    if(p->num_of_keys == 0){
        if(!p->is_leaf){
            file_read_page(p->right_page_offset, &child);
            child->page_offset = 0;
            header->root_page_offset = p->right_page_offset;
            file_write_page(p->right_page_offset, child);
            file_free_page(pn);
            free(child);
        }
        else{
            header->root_page_offset = 0;
            file_free_page(pn);
        }
    }
    free(p);
    return 0;
}

int coalesce_nodes(pagenum_t pn, pagenum_t neighbor_num, int neighbor_index, int64_t k_prime){
    int i, j, neighbor_insertion_index, n_end;
    page * tmp;
    page * p, * neighbor;
    pagenum_t tmp_num;

    file_read_page(pn, &p);
    file_read_page(neighbor_num, &neighbor);

    if(neighbor_index == -2){
        tmp = p;
        tmp_num = pn;
        p = neighbor;
        pn = neighbor_num;
        neighbor = tmp;
        neighbor_num = tmp_num;
    }
    neighbor_insertion_index = neighbor->num_of_keys;
        
    if(!p->is_leaf){
        neighbor->internal[neighbor_insertion_index].key = k_prime;
        neighbor->internal[neighbor_insertion_index].page_offset = p->right_page_offset;
        neighbor->num_of_keys++;

        n_end = p->num_of_keys;

        for(i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++){
            neighbor->internal[i] = p->internal[j];
            neighbor->num_of_keys++;
            p->num_of_keys--;
        }
    
        file_read_page(neighbor->right_page_offset, &tmp);
        tmp->page_offset = neighbor_num;
        file_write_page(neighbor->right_page_offset, tmp);
        for(i = 0; i < neighbor->num_of_keys; i++){
            file_read_page(neighbor->internal[i].page_offset, &tmp);
            tmp->page_offset = neighbor_num;
            file_write_page(neighbor->internal[i].page_offset, tmp);
        }
        file_write_page(neighbor_num, neighbor);
        file_free_page(pn);
        free(tmp);
    }
    else{
        for(i = neighbor_insertion_index, j = 0; j < p->num_of_keys; i++, j++){
            neighbor->leaf[i] = p->leaf[j];
            neighbor->num_of_keys++;
        }
        neighbor->right_page_offset = p->right_page_offset;
        file_write_page(neighbor_num, neighbor);
        file_free_page(pn);

    }

    i = delete_entry(p->page_offset, k_prime);
    free(neighbor);
    free(p);
    return 0;
}

int delete_entry(pagenum_t pn, int64_t key){
    pagenum_t neighbor_num;
    page * neighbor_insertion_indexghbor, * p, * parent;
    int neighbor_index;
    int k_prime_index;
    int64_t k_prime;
    int capacity;

    remove_entry_from_page(pn, key);
    if(pn == header->root_page_offset)
        return adjust_root(pn);
    file_read_page(pn, &p);

    if(p->num_of_keys > 0)
        return 0;

    file_read_page(p->page_offset, &parent);
    neighbor_index = get_neighbor_index(pn);
    if(neighbor_index == -2){                       
        neighbor_num = parent->internal[0].page_offset;
        k_prime_index = 0;
    }
    else if(neighbor_index == -1){
        neighbor_num = parent->right_page_offset;
        k_prime_index = 0;
    }
    else{
        neighbor_num = parent->internal[neighbor_index].page_offset;
        k_prime_index = neighbor_index + 1;
    }

    k_prime = parent->internal[k_prime_index].key;
    
    return coalesce_nodes(pn, neighbor_num, neighbor_index, k_prime);


}

int delete(int64_t key){
    pagenum_t pn;
    pn = find_leaf(key);
    if(pn != 0){
        return delete_entry(pn, key);
    }
    return -1;
}