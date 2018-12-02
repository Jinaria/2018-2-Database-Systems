#include "bpt.h"

buffer_manager bm;
int leaf_order = LEAF_ORDER;
int internal_order = INTERNAL_ORDER;
pagenum_t queue[100];
int start = 0, end = -1;

int fd[TABLE_LIMIT + 1];
int table_count = 0;
int next_table_num = 1;

vector<query_t> query_set;
int joined_table[TABLE_LIMIT + 1] = {-1, };
int table_num = 0;
int max_table_size = 0;

table table_list[TABLE_LIMIT + 1];

vector<vector<pair<int64_t , int64_t> > > index_record;
int64_t index_record_size = 0;
// int thd_cnt = 0;
// vector< vector<pair<int64_t, int64_t> > > index_record, *temp_v;
// #define INDEX(row, table) index_record[row][table].first
// #define KEY(row, table) index_record[row][table].second

// int nthread = 1, thd_num;
// Buffer Manager

void print_table(int table_id){
    int k = 0;
    for(int i = 0; i < table_list[table_id].index_size; i++){
        for(int j = 0; j < table_list[table_id].column; j++){
            // k++;
            cout << table_list[table_id].record[i][j] << ' ';
        }
        cout << endl << k << endl;
    }
}

void print_tree(int table_id) {
    pagenum_t *queue;
    int i;
    int front = 0;
    int rear = 0;
    page * header = table_list[table_id].header;
    if (header->root_page_offset == 0) {
        printf("Empty tree.\n");
        return;
    }
    
    queue = (pagenum_t*)malloc(sizeof(pagenum_t) * header->num_of_pages);
    queue[rear] = header->root_page_offset;
    rear++;
    queue[rear] = 0;
    rear++;
    while (front < rear) {
        pagenum_t page_offset = queue[front];
        front++;
        if (page_offset == 0) {
            printf("\n");
            
            if (front == rear) break;

            queue[rear] = 0;
            rear++;
            continue;
        }
        
        buffer * page;
        page = get_buffer(table_id, page_offset / PAGE_SIZE);
        if (page->frame->is_leaf) {
            for (i = 0; i < page->frame->num_of_keys; i++) {
                cout << page->frame->leaf[i].key << ' ';
            }
            printf("| ");
        } else {
            queue[rear] = page->frame->left_page_offset;
            rear++;
            for (i = 0; i < page->frame->num_of_keys; i++) {
                cout << page->frame->internal[i].key << ' ';
                queue[rear] = page->frame->internal[i].page_offset;
                rear++;
            }
            printf("| ");
        }
    }
    free(queue);
}

int init_db(int num_buf){
    int i;
    bm.capacity = num_buf;
    bm.buffer_num = 0;
    bm.bf = new buffer[num_buf]();
    if(bm.bf == NULL)
        return -1;
    for(i = 0; i < num_buf; i++){
        bm.bf[i].table_id = -1;
        bm.bf[i].page_num = -1;
    }
    for(int i = 0; i <= TABLE_LIMIT; i++)
        fd[i] = -1;
    bm.lru = -1;
    bm.mru = -1;
    
    return 0;
}

int shutdown_db(){
    int i;
    for(i = 1; i <= TABLE_LIMIT; i++){
        close_table(i);
    }
    delete[] bm.bf;
    table_count = 0;
    next_table_num = 0;

    return 0;
}

buffer * get_buffer(int table_id, pagenum_t pn){
    int i = 0;
    if(fd[table_id] == -1){
        printf("not opened table\n");
        exit(EXIT_FAILURE);
        // return NULL;
    }
    if(bm.lru == -1){
        file_read_page(table_id, pn, &(bm.bf[i].frame));
        bm.bf[i].table_id = table_id;
        bm.bf[i].page_num = pn;
        bm.bf[i].is_dirty = false;
        bm.bf[i].pin_count = 1;
        bm.bf[i].next = -1;
        bm.bf[i].prev = 0;
        bm.lru = 0;
        bm.mru = 0;
        bm.buffer_num++;

        return &bm.bf[i];
    }
    for(i = bm.mru; i != -1; i = bm.bf[i].next){
        
        if(bm.bf[i].table_id == table_id && bm.bf[i].page_num == pn){
            bm.bf[i].pin_count++;
            if(i == bm.mru){
                return &bm.bf[i];
            }
            else if(i == bm.lru){
                bm.lru = bm.bf[bm.lru].prev;
                bm.bf[bm.lru].next = -1;

                bm.bf[i].prev = -1;
                bm.bf[i].next = bm.mru;
                bm.bf[bm.mru].prev = i;
                bm.mru = i;

                return &bm.bf[i];
            }
            else{
                bm.bf[bm.bf[i].prev].next = bm.bf[i].next;
                bm.bf[bm.bf[i].next].prev = bm.bf[i].prev;
                bm.bf[bm.mru].prev = i;
                bm.bf[i].next = bm.mru;
                bm.bf[i].prev = -1;
                bm.mru = i;

                return &bm.bf[i];
            }
        }
    }

    if(bm.capacity == bm.buffer_num){
        if(bm.bf[bm.lru].is_dirty){
            file_write_page(bm.bf[bm.lru].table_id, bm.bf[bm.lru].page_num, bm.bf[bm.lru].frame);
        }
        file_read_page(table_id, pn, &bm.bf[bm.lru].frame);
        bm.bf[bm.lru].table_id = table_id;
        bm.bf[bm.lru].page_num = pn;
        bm.bf[bm.lru].is_dirty = false;
        bm.bf[bm.lru].pin_count = 1;
       

        bm.bf[bm.mru].prev = bm.lru;
        bm.bf[bm.lru].next = bm.mru;
        bm.mru = bm.lru;
        bm.lru = bm.bf[bm.lru].prev;
        bm.bf[bm.lru].next = -1;
        bm.bf[bm.mru].prev = -1;
        return &bm.bf[bm.mru];
    }
    else if(bm.buffer_num < bm.capacity){
        file_read_page(table_id, pn, &(bm.bf[bm.buffer_num].frame));
        bm.bf[bm.buffer_num].table_id = table_id;
        bm.bf[bm.buffer_num].page_num = pn;
        bm.bf[bm.buffer_num].is_dirty = false;
        bm.bf[bm.buffer_num].pin_count = 1;
        bm.bf[bm.buffer_num].next = bm.mru;
        bm.bf[bm.buffer_num].prev = -1;
        bm.bf[bm.mru].prev = bm.buffer_num;
        bm.mru = bm.buffer_num;
        bm.buffer_num++;

        return &bm.bf[bm.mru];
    }
    return NULL;
}


// Find

buffer * find_leaf(int table_id, int64_t key){
    int i;
    pagenum_t p;
    page * header = table_list[table_id].header;
    p = header->root_page_offset / PAGE_SIZE;
    if(p == 0)
        return NULL;
    buffer * buf = get_buffer(table_id, p);
    
    // file_read_page(p, &c);
    if(buf == NULL)
        return NULL;
    while(!buf->frame->is_leaf){
        i = -1;
        if(key < buf->frame->internal[0].key){
            buf->pin_count--;
            buf = get_buffer(table_id, buf->frame->left_page_offset / PAGE_SIZE);
            // p = c->right_page_offset;
            // file_read_page(p, &c);
            continue;
        }
        for(i = buf->frame->num_of_keys - 1; i > -1; i--){
            if(key >= buf->frame->internal[i].key)
                break;
        }
        buf->pin_count--;
        buf = get_buffer(table_id, buf->frame->left_page_offset / PAGE_SIZE);
        // p = c->internal[i].page_offset;
        // file_read_page(p, &c);
    }

    return buf;
}

int64_t * find(int table_id, int64_t key){
    int i = 0;
    buffer * buf;
    // page * c;
    // pagenum_t p = find_leaf(key);

    // char * value = (char *)malloc(MAX_LENGTH);
    int64_t * value = new int64_t[VALUE_SIZE];
    buf = find_leaf(table_id, key);
     // file_read_page(p, &c);


    if(buf == NULL) return NULL;
    for(i = 0; i < buf->frame->num_of_keys; i++){
        if(buf->frame->leaf[i].key == key) break;
    }
    if(i == buf->frame->num_of_keys){
        printf("no key\n");
        return NULL;
    }
    // strcpy(value, buf->frame->leaf[i].value);
    
    memcpy(value, buf->frame->leaf[i].value, sizeof(int64_t) * VALUE_SIZE);
    buf->pin_count--;
    
    return value;
}

int cut(int length){
    if(length % 2 == 0)
        return length / 2;
    else
        return length / 2 + 1;
}

// File I/O

int open_table(char * pathname, int num_column){
    int temp = 0, id, i;
    page * header;
    if(table_count >= 10){
        printf("table is full\n");
        return -1;
    }
    // header = (page*)calloc(1, PAGE_SIZE);
    header = new page();
    id = next_table_num;
    for(i = 1; i <= TABLE_LIMIT; i++){
        if(i == id)
            continue;
        if(fd[i] == -1){
            next_table_num = i;
            break;
        }
    }
    fd[id] = open(pathname, O_RDWR | O_CREAT | O_EXCL | O_SYNC, 0777);

    if(fd[id] > 0){
        if(num_column < 2 || num_column > 16){
            cout << "column range out\n";

            return -1;
        }

        header->num_of_columns = num_column;
        header->num_of_pages = 1;
        table_list[id].header = header;
        temp = pwrite(fd[id], header, PAGE_SIZE, SEEK_SET);
        
        if(temp < PAGE_SIZE){
            printf("Failed to write header_page\n");
            exit(EXIT_FAILURE);
        }
        table_count++;
        return id;
    }
    fd[id] = open(pathname, O_RDWR | O_SYNC);
    if(fd[id] > 0){
        temp = pread(fd[id], header, PAGE_SIZE, SEEK_SET);
        if (temp < PAGE_SIZE) {
            printf("Failed to read header_page\n");
            exit(EXIT_FAILURE);
        }
        table_list[id].header = header;
        table_count++;
        read_table(id);
        return id;
    }
    printf("Failed to open\n");
    return -1;
}

int close_table(int table_id){
    int i, buffer_num = bm.buffer_num;

    for(i = 0; i < buffer_num; i++){
        if(bm.bf[i].table_id == table_id && bm.bf[i].is_dirty/* && bm.bf[i].pin_count == 0*/){
            file_write_page(table_id, bm.bf[i].page_num, bm.bf[i].frame);
            bm.buffer_num--;
        }
    }
    if(table_list[table_id].header != NULL){
        file_write_page(table_id, 0, table_list[table_id].header);
        delete table_list[table_id].header;
    }
    table_list[table_id].column = 0;
    table_list[table_id].index_size = 0;
    for(int i = 0; i < table_list[table_id].record.size(); i++){
        delete table_list[table_id].record[i];
    }
    

    fd[table_id] = -1;
    table_count--;
    for(i = 1; i <= TABLE_LIMIT; i++){
        if(fd[i] == -1){
            next_table_num = i;
            break;
        }
    }

    return 0;
}

pagenum_t file_alloc_page(int table_id){
    pagenum_t pn;
    page * p;
    page * header = table_list[table_id].header;
    if(header->free_page_offset == 0){
        return increase_free_page(table_id);
    }
    pn = header->free_page_offset / PAGE_SIZE;
    file_read_page(table_id, pn, &p);
    header->free_page_offset = p->page_offset;
    
    delete p;
    return pn;
}

void file_free_page(buffer * buf){
    page * header = table_list[buf->table_id].header;
    buf->frame->page_offset = header->free_page_offset;
    if(buf->frame->is_leaf)
        buf->frame->right_page_offset = 0;
    else
        buf->frame->left_page_offset = 0;
    buf->frame->is_leaf = 1;
    buf->frame->num_of_keys = 0;
    header->free_page_offset = buf->page_num * PAGE_SIZE;
    buf->is_dirty = true;
    buf->pin_count--;
}

pagenum_t increase_free_page(int table_id){
    int i, temp;
    page * header = table_list[table_id].header;
    pagenum_t pn = header->num_of_pages;
    // page * p = (page*)calloc(1, PAGE_SIZE);
    page * p = new page();
    p->page_offset = header->free_page_offset;
    p->num_of_keys = 0;
    p->is_leaf = 1;
    header->free_page_offset = pn * PAGE_SIZE;
    header->num_of_pages++;
    file_write_page(table_id, pn, p);
    // free(p);
    delete p;
    return pn;
}

void file_read_page(int table_id, pagenum_t pagenum, page ** dest){
    int temp;
    // *dest = (page * )calloc(1, PAGE_SIZE);
    *dest = new page();
    temp = pread(fd[table_id], *dest, PAGE_SIZE, PAGE_SIZE * pagenum);
    if(temp < PAGE_SIZE){
        printf("Failed to read from file_read_page\n");
        exit(EXIT_FAILURE);
    }
}

void file_write_page(int table_id, pagenum_t pagenum, const page * src){
    
    int temp;
    temp = pwrite(fd[table_id], src, PAGE_SIZE, pagenum * PAGE_SIZE);
    if(temp < PAGE_SIZE){
        printf("Failed to write from file_write_page\n");
        exit(EXIT_FAILURE);
    }
}

//Insertion

pagenum_t get_left_index(buffer * parent, buffer * left){
    int left_index = 0;
    // file_read_page(parent_num, &parent);

    if(parent->frame->left_page_offset == left->page_num * PAGE_SIZE)
        return -1;
    while(left_index <= parent->frame->num_of_keys - 1 &&
            parent->frame->internal[left_index].page_offset != left->page_num * PAGE_SIZE)
        left_index++;
    return left_index;
}

int start_new_tree(int table_id, int64_t key, int64_t * value){
    int i;
    page * header = table_list[table_id].header;
    buffer * root_buf;
    if(header->free_page_offset == 0){
        for(i = 0; i < 10; i++){
            increase_free_page(table_id);
        }   
    }
    
    header->root_page_offset = file_alloc_page(table_id) * PAGE_SIZE;
    // file_read_page(header->root_page_offset, &root);
    root_buf = get_buffer(table_id, header->root_page_offset / PAGE_SIZE);

    root_buf->frame->page_offset = 0;
    root_buf->frame->is_leaf = true;
    root_buf->frame->num_of_keys = 1;
    root_buf->frame->right_page_offset = 0;
    root_buf->frame->leaf[0].key = key;
    // strcpy(root_buf->frame->leaf[0].value, value);
    memcpy(root_buf->frame->leaf[0].value, value, sizeof(int64_t) * header->num_of_columns);
    root_buf->pin_count--;
    root_buf->is_dirty = true;
    return 0;
}

int insert_into_leaf(buffer * leaf, int64_t key, int64_t * value){
    int i, insertion_point = 0;
    
    while(insertion_point < leaf->frame->num_of_keys && leaf->frame->leaf[insertion_point].key < key)
        insertion_point++;

    for(i = leaf->frame->num_of_keys; i > insertion_point; i--){
        leaf->frame->leaf[i] = leaf->frame->leaf[i - 1];
    }
    leaf->frame->leaf[insertion_point].key = key;
    // strcpy(leaf->frame->leaf[insertion_point].value, value);
    memcpy(leaf->frame->leaf[insertion_point].value, value, sizeof(int64_t) * VALUE_SIZE);
    
    leaf->frame->num_of_keys++;
    
    leaf->pin_count--;
    leaf->is_dirty = true;

    return 0;

}

int insert_into_leaf_after_splitting(int table_id, buffer * leaf, int64_t key, int64_t * value){
    
    pagenum_t new_leaf_num;
    page * header;
    buffer * new_leaf;
    int insertion_point, split, i, j;
    int64_t new_key;
    leaf_record temp[leaf_order];
    header = table_list[table_id].header;
    
    // file_read_page(leaf_num, &leaf);
    new_leaf_num = file_alloc_page(table_id);
    // file_read_page(new_leaf_num, &new_leaf);
    new_leaf = get_buffer(table_id, new_leaf_num);

    insertion_point = 0;
    while(insertion_point < leaf_order - 1 && leaf->frame->leaf[insertion_point].key < key){
        insertion_point++;
    }

    for(i = 0, j = 0; i < leaf->frame->num_of_keys; i++, j++){
        if(j == insertion_point) j++;
        temp[j] = leaf->frame->leaf[i];
    }
    temp[insertion_point].key = key;
    // strcpy(temp[insertion_point].value, value);
    memcpy(temp[insertion_point].value, value, sizeof(int64_t) * VALUE_SIZE);
    leaf->frame->num_of_keys = 0;

    split = cut(leaf_order);

    for(i = 0; i < split; i++){
        leaf->frame->leaf[i] = temp[i];
        leaf->frame->num_of_keys++;
    }
    for(i = split, j = 0; i < leaf_order; i++, j++){
        new_leaf->frame->leaf[j] = temp[i];
        new_leaf->frame->num_of_keys++;
    }
    new_leaf->frame->right_page_offset = leaf->frame->right_page_offset;
    leaf->frame->right_page_offset = new_leaf_num * PAGE_SIZE;
    
    for(i = leaf->frame->num_of_keys; i < leaf_order; i++){
        leaf->frame->leaf[i].key = 0;
        for(int i = 0; i < VALUE_SIZE; i++){
            leaf->frame->leaf[i].value[i] = 0;
        }
    }
    for(i = new_leaf->frame->num_of_keys; i < leaf_order; i++){
        new_leaf->frame->leaf[i].key = 0;
        for(int i = 0; i < VALUE_SIZE; i++){
            new_leaf->frame->leaf[i].value[i] = 0;
        }
    }

    new_leaf->frame->page_offset = leaf->frame->page_offset;
    new_key = new_leaf->frame->leaf[0].key;
    
    return insert_into_parent(table_id, leaf, new_key, new_leaf);
}

int insert_into_parent(int table_id, buffer * left, int64_t key, buffer * right){
    
    int left_index;
    buffer * parent;
    // page * left;

    // file_read_page(left_num, &left);
    if(left->frame->page_offset == 0)
        return insert_into_new_root(table_id, left, key, right);

    // file_read_page(left->page_offset, &parent);
    parent = get_buffer(table_id, left->frame->page_offset / PAGE_SIZE);

    left_index = get_left_index(parent, left);
    
    if(parent->frame->num_of_keys < internal_order - 1){
        left->is_dirty = true;
        left->pin_count--;
        return insert_into_page(parent, left_index, key, right);
    }

    left->is_dirty = true;
    left->pin_count--;
    return insert_into_page_after_splitting(parent, left_index, key, right);
}

int insert_into_new_root(int table_id, buffer * left, int64_t key, buffer * right){
    
    pagenum_t root_num = file_alloc_page(table_id);
    buffer * root_buf = get_buffer(table_id, root_num);
    page * header = table_list[table_id].header;

    header->root_page_offset = root_num * PAGE_SIZE;
    root_buf->frame->page_offset = 0;
    root_buf->frame->is_leaf = false;
    root_buf->frame->num_of_keys = 1;
    root_buf->frame->left_page_offset = left->page_num * PAGE_SIZE;
    root_buf->frame->internal[0].key = key;
    root_buf->frame->internal[0].page_offset = right->page_num * PAGE_SIZE;
    left->frame->page_offset = root_num * PAGE_SIZE;
    right->frame->page_offset = root_num * PAGE_SIZE;
    
    root_buf->is_dirty = true;
    root_buf->pin_count--;
    left->is_dirty = true;
    left->pin_count--;
    right->is_dirty = true;
    right->pin_count--;

    return 0;   
}

int insert_into_page(buffer * parent, int left_index, int64_t key, buffer * right){
    
    int i;

    for(i = parent->frame->num_of_keys - 1; i > left_index; i--){
        parent->frame->internal[i + 1] = parent->frame->internal[i];
    }

    parent->frame->internal[left_index + 1].key = key;
    parent->frame->internal[left_index + 1].page_offset = right->page_num * PAGE_SIZE;
    parent->frame->num_of_keys++;
    
    parent->is_dirty = true;
    parent->pin_count--;
    right->is_dirty = true;
    right->pin_count--;

    return 0;
}

int insert_into_page_after_splitting(buffer * old_page, int left_index, int64_t key, buffer * right){
   
    int i, j, split;
    int64_t k_prime;
    internal_record temp[internal_order];
    pagenum_t first_page_offset, new_page_num;
    buffer * new_page, * child;


    // file_read_page(old_page_num, &old_page);

    first_page_offset = old_page->frame->left_page_offset;
    for(i = 0, j = 0; i < old_page->frame->num_of_keys; i++, j++){
        if(j == left_index + 1) j++;
        temp[j] = old_page->frame->internal[i];
    }
    temp[left_index + 1].key = key;
    temp[left_index + 1].page_offset = right->page_num * PAGE_SIZE;
    

    split = cut(internal_order);
    new_page_num = file_alloc_page(old_page->table_id);
    new_page = get_buffer(old_page->table_id, new_page_num);
    old_page->frame->num_of_keys = 0;

    old_page->frame->left_page_offset = first_page_offset;
    for(i = 0; i < split - 1; i++){
        old_page->frame->internal[i] = temp[i];
        old_page->frame->num_of_keys++;
    }
    new_page->frame->num_of_keys = 0;
    new_page->frame->left_page_offset = temp[split - 1].page_offset;
    new_page->frame->is_leaf = 0;
    k_prime = temp[split - 1].key;
    for(i = split, j = 0; i < internal_order; i++, j++){
        new_page->frame->internal[j] = temp[i];
        new_page->frame->num_of_keys++;
    }
    new_page->frame->page_offset = old_page->frame->page_offset;
    // file_read_page(new_page->right_page_offset, &child);
    child = get_buffer(new_page->table_id, new_page->frame->left_page_offset / PAGE_SIZE);
    child->frame->page_offset = new_page_num * PAGE_SIZE;
    child->is_dirty = true;
    child->pin_count--;
    // file_write_page(new_page->right_page_offset, child);
    for(i = 0; i < new_page->frame->num_of_keys; i++){
        child = get_buffer(new_page->table_id, new_page->frame->internal[i].page_offset / PAGE_SIZE);
        // file_read_page(new_page->internal[i].page_offset, &child);
        child->frame->page_offset = new_page_num * PAGE_SIZE;
        child->is_dirty = true;
        child->pin_count--;
    }

    return insert_into_parent(old_page->table_id, old_page, k_prime, new_page);
}

int insert(int table_id, int64_t key, int64_t * value){
    
    page * header = table_list[table_id].header;
    buffer * leaf;

    if(header == NULL){
        printf("Table is not opened\n");
        return -1;
    }
    if(header->root_page_offset == 0){
        return start_new_tree(table_id, key, value);
    }
    if(find(table_id, key) != NULL){
        return -1;
    }
    leaf = find_leaf(table_id, key);
    if(leaf->frame->num_of_keys < leaf_order - 1){
        insert_into_leaf(leaf, key, value);
        return 0;
    }
    insert_into_leaf_after_splitting(table_id, leaf, key, value);
    
    return 0;
}

//Deletion

int get_neighbor_index(buffer * buf){
    int i;
    buffer * parent = get_buffer(buf->table_id, buf->frame->page_offset / PAGE_SIZE);
    if(parent->frame->left_page_offset == buf->page_num * PAGE_SIZE){
        parent->pin_count--;
        
        return -2;
    }
    for(i = 0; i < parent->frame->num_of_keys; i++){
        if(parent->frame->internal[i].page_offset == buf->page_num * PAGE_SIZE){
            parent->pin_count--;
            return i - 1;
        }
    }
}

void remove_entry_from_page(buffer * buf, int64_t key){
    int i;
     

    i = 0;
    if(buf->frame->is_leaf){
        while(buf->frame->leaf[i].key != key)
            i++;
        for(++i; i < buf->frame->num_of_keys; i++)
            buf->frame->leaf[i - 1] = buf->frame->leaf[i];
    }
    else{

        while(buf->frame->internal[i].key != key)
            i++;
        for(++i; i < buf->frame->num_of_keys; i++)
            buf->frame->internal[i - 1] = buf->frame->internal[i];
    }   
    buf->frame->num_of_keys--;
    
}

int adjust_root(buffer * buf){
    
    buffer * child;
    page * header = table_list[buf->table_id].header;
    if(buf->frame->num_of_keys == 0){
        if(!buf->frame->is_leaf){
            child = get_buffer(buf->table_id, buf->frame->left_page_offset / PAGE_SIZE);
            child->frame->page_offset = 0;
            header->root_page_offset = buf->frame->left_page_offset / PAGE_SIZE;
            file_free_page(buf);

            child->is_dirty = true;
            child->pin_count--;

        }
        else{
            header->root_page_offset = 0;
            file_free_page(buf);
        }
    }
    return 0;
}

int coalesce_nodes(buffer * buf, pagenum_t neighbor_num, int neighbor_index, int64_t k_prime){
    int i, j, neighbor_insertion_index, n_end;
    pagenum_t parent_num;

    buffer * neighbor, * tmp, * parent;
    pagenum_t tmp_num;

    neighbor = get_buffer(buf->table_id, neighbor_num);

    if(neighbor_index == -2){
        tmp = buf;
        buf = neighbor;
        neighbor = tmp;
    }
    neighbor_insertion_index = neighbor->frame->num_of_keys;
    parent_num = buf->page_num;
    if(!buf->frame->is_leaf){
        neighbor->frame->internal[neighbor_insertion_index].key = k_prime;
        neighbor->frame->internal[neighbor_insertion_index].page_offset = buf->frame->left_page_offset;
        neighbor->frame->num_of_keys++;

        n_end = buf->frame->num_of_keys;

        for(i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++){
            neighbor->frame->internal[i] = buf->frame->internal[j];
            neighbor->frame->num_of_keys++;
            buf->frame->num_of_keys--;
        }
    
        tmp = get_buffer(buf->table_id, neighbor->frame->left_page_offset / PAGE_SIZE);
        tmp->frame->page_offset = neighbor_num * PAGE_SIZE;
        tmp->is_dirty = true;
        tmp->pin_count--;
        for(i = 0; i < neighbor->frame->num_of_keys; i++){
            tmp = get_buffer(buf->table_id, neighbor->frame->internal[i].page_offset / PAGE_SIZE);
            tmp->frame->page_offset = neighbor_num * PAGE_SIZE;
            tmp->is_dirty = true;
            tmp->pin_count--;
        }
        
    }
    else{
        for(i = neighbor_insertion_index, j = 0; j < buf->frame->num_of_keys; i++, j++){
            neighbor->frame->leaf[i] = buf->frame->leaf[j];
            neighbor->frame->num_of_keys++;
        }
        neighbor->frame->right_page_offset = buf->frame->right_page_offset;
       
    }
    neighbor->is_dirty = true;
    neighbor->pin_count--;
    buf->is_dirty = true;
    buf->pin_count--;
    parent = get_buffer(buf->table_id, parent_num);
    i = delete_entry(buf->table_id, parent, k_prime);

    return 0;
}

int delete_entry(int table_id, buffer * buf, int64_t key){
    pagenum_t neighbor_num;
    buffer * parent;
    page * header = table_list[table_id].header;
    int neighbor_index;
    int k_prime_index;
    int64_t k_prime;
    int capacity;


    remove_entry_from_page(buf, key);
    if(buf->page_num * PAGE_SIZE == header->root_page_offset)
        return adjust_root(buf);

    if(buf->frame->num_of_keys > 0){
        buf->is_dirty = true;
        buf->pin_count--;
        return 0;
    }

    parent = get_buffer(table_id, buf->frame->page_offset / PAGE_SIZE);
    neighbor_index = get_neighbor_index(buf);
    if(neighbor_index == -2){                       
        neighbor_num = parent->frame->internal[0].page_offset / PAGE_SIZE;
        k_prime_index = 0;
    }
    else if(neighbor_index == -1){
        neighbor_num = parent->frame->left_page_offset / PAGE_SIZE;
        k_prime_index = 0;
    }
    else{
        neighbor_num = parent->frame->internal[neighbor_index].page_offset / PAGE_SIZE;
        k_prime_index = neighbor_index + 1;
    }

    k_prime = parent->frame->internal[k_prime_index].key;
    parent->pin_count--;
    return coalesce_nodes(buf, neighbor_num, neighbor_index, k_prime);


}

int erase(int table_id, int64_t key){
    pagenum_t pn;
    buffer * buf;
    buf = find_leaf(table_id, key);
    if(pn != 0){
        return delete_entry(table_id, buf, key);
    }
    return -1;
}

void swap(int * a, int * b){
    int temp = *a;
    *a = *b;
    *b = temp;
}
query_t parse_query(string q){
    int t1, t2, c1, c2;
    stringstream qs(q);

    string s1, s2;
    getline(qs, s1, '=');
    stringstream ts1(s1);
    
    getline(ts1, s2, '.');
    t1 = atoi(s2.c_str());
    getline(ts1, s2, '.');
    c1 = atoi(s2.c_str());

    getline(qs, s1, '=');
    stringstream ts2(s1);

    getline(ts2, s2, '.');
    t2 = atoi(s2.c_str());
    getline(ts2, s2, '.');
    c2 = atoi(s2.c_str());

    if(t1 > t2){
        swap(&t1, &t2);
        swap(&c1, &c2);
    }
    query_t temp;
    temp.t1 = t1;
    temp.c1 = c1;
    temp.t2 = t2;
    temp.c2 = c2;

    return temp;
}

vector<query_t> sort_query(vector<query_t> v){
    vector<query_t> q;
    int num = v.size();
    int index[9] = {0, };
    index[0] = 1;
    int count = 1;
    int i = 1;
    int cur_q = 0;
    q.push_back(v.at(0));
    int cnt = 0;

    while(count < num){
        if(i == 0)
            cur_q++;
        if(index[i] == 0){
            if(q.at(cur_q).t1 == v.at(i).t1 || q.at(cur_q).t1 == v.at(i).t2 || q.at(cur_q).t2 == v.at(i).t1 || q.at(cur_q).t2 == v.at(i).t2){
                q.push_back(v.at(i));
                index[i] = 1;
                count++;
            }
            
        }
        i = (i + 1) % num;
    }

    return q;
}

int parse(string query){
    int cnt = 0;
    stringstream ss(query);
    string temp;
    while(getline(ss, temp, '&')){
        cnt++;
        query_set.push_back(parse_query(temp));
    }
    query_set = sort_query(query_set);

    return cnt;
}

void read_table(int table_id){
    page * header = table_list[table_id].header;
    int64_t * row;
    if(header->root_page_offset == 0){
        return;
    }
    buffer * temp = get_buffer(table_id, header->root_page_offset / PAGE_SIZE);
    while(!temp->frame->is_leaf){
        temp->pin_count = 0;
        temp = get_buffer(table_id, temp->frame->left_page_offset / PAGE_SIZE);
    }
    while(temp->frame->right_page_offset != 0){
        
        for(int i = 0; i < temp->frame->num_of_keys; i++){
            row = new int64_t[header->num_of_columns];
            row[0] = temp->frame->leaf[i].key;
            for(int j = 1; j < header->num_of_columns; j++){
                row[j] = temp->frame->leaf[i].value[j - 1];
            }
            table_list[table_id].record.push_back(row);
        }
        
        temp->pin_count = 0;
        temp = get_buffer(table_id, temp->frame->right_page_offset / PAGE_SIZE);
    }
    
    for(int i = 0; i < temp->frame->num_of_keys; i++){
        
        row = new int64_t[header->num_of_columns];
        row[0] = temp->frame->leaf[i].key;
        for(int j = 1; j < header->num_of_columns; j++){
            row[j] = temp->frame->leaf[i].value[j - 1];
        }
        table_list[table_id].record.push_back(row);
    }
    table_list[table_id].column = header->num_of_columns;
    table_list[table_id].index_size = table_list[table_id].record.size();
    if(max_table_size <= table_list[table_id].index_size)
        max_table_size = table_list[table_id].index_size;
    
    temp->pin_count = 0;
}


void join_one_query(query_t q){ //hash join
    vector<pair<int64_t, int64_t> > v;
    int64_t table_size = table_list[q.t1].index_size > table_list[q.t2].index_size ? table_list[q.t1].index_size : table_list[q.t2].index_size;
    
    vector<vector<pair<int64_t, int64_t> > > hash_table(table_size * 2);
    int64_t cnt = 0;
    if(table_num == 0){
        int sum = 0;
        joined_table[q.t1] = table_num++;
        joined_table[q.t2] = table_num++;
        // index_record.reserve(table_list[q.t1].index_size * table_list[q.t2].index_size);
        // cout << "11\n" << endl;
        for(int64_t i = 0; i < table_list[q.t1].index_size; i++){
            int64_t hashed = table_list[q.t1].record[i][q.c1 - 1] % table_size;
            hash_table[hashed].push_back(make_pair(i, table_list[q.t1].record[i][q.c1 - 1]));
        }
        // cout << "12\n" << endl;
        for(int64_t i = 0; i < table_list[q.t2].index_size; i++){
            int64_t hashed = table_list[q.t2].record[i][q.c2 - 1] % table_size;
            // cout << 3 << endl;
            for(int64_t j = 0; j < hash_table[hashed].size(); j++){
                // cout << 4 << endl;
                // cout << hash_table[hashed][j].second << endl;
                // cout << table_list[q.t2].record[i][q.c2 - 1] << endl;

                if(hash_table[hashed][j].second == table_list[q.t2].record[i][q.c2 - 1]){
                    // cout << 1 << endl;
                    v.push_back(make_pair(hash_table[hashed][j].first, table_list[q.t1].record[hash_table[hashed][j].first][0]));

                    v.push_back(make_pair(i, table_list[q.t2].record[i][0]));
                    index_record.push_back(v);
                    v.clear();
                    // cout << cnt << ' ' << sum << endl;

                }
            }
            // cout << 2 << endl;
        }
        // cout << "13\n" << endl;
        // index_record_size = cnt;
    }
    else{
        if(joined_table[q.t1] == -1){
            vector<vector<pair<int64_t, int64_t> > > tv;
            
            joined_table[q.t1] = table_num++;
            // cout << "21\n" << endl;
            for(int64_t i = 0; i < index_record.size(); i++){
                int64_t hashed = table_list[q.t2].record[index_record[i][joined_table[q.t2]].first][q.c2 - 1] % table_size;
                hash_table[hashed].push_back(make_pair(i, table_list[q.t2].record[index_record[i][joined_table[q.t2]].first][q.c2 - 1]));
            }
            // cout << "22\n" << endl;
            for(int64_t i = 0; i < table_list[q.t1].index_size; i++){

                int64_t hashed = table_list[q.t1].record[i][q.c1 - 1] % table_size;
                for(int64_t j = 0; j < hash_table[hashed].size(); j++){
                    if(hash_table[hashed][j].second == table_list[q.t1].record[i][q.c1 - 1]){

                        index_record[hash_table[hashed][j].first].push_back(make_pair(i, table_list[q.t1].record[i][0]));
                        tv.push_back(index_record[hash_table[hashed][j].first]);
                        index_record[hash_table[hashed][j].first].pop_back();
                    }
                }
            }
            // cout << "23\n" << endl;
            // index_record_size = cnt;
            index_record = tv;
        }
        else{
            vector<vector<pair<int64_t, int64_t> > > tv;
            joined_table[q.t2] = table_num++;
            // cout << "31\n" << endl;
            for(int64_t i = 0; i < index_record.size(); i++){
                int64_t hashed = table_list[q.t1].record[index_record[i][joined_table[q.t1]].first][q.c1 - 1] % table_size;
                hash_table[hashed].push_back(make_pair(i, table_list[q.t1].record[index_record[i][joined_table[q.t1]].first][q.c1 - 1]));
            }
            // cout << "32\n" << endl;
            for(int64_t i = 0; i < table_list[q.t2].index_size; i++){
             
                int64_t hashed = table_list[q.t2].record[i][q.c2 - 1] % table_size;
                for(int64_t j = 0; j < hash_table[hashed].size(); j++){                
                    if(hash_table[hashed][j].second == table_list[q.t2].record[i][q.c2 - 1]){
                        
                        index_record[hash_table[hashed][j].first].push_back(make_pair(i, table_list[q.t2].record[i][0]));
                        tv.push_back(index_record[hash_table[hashed][j].first]);
                        index_record[hash_table[hashed][j].first].pop_back();
                    }
                } 
             
            }
            // cout << "33\n" << endl;
            // index_record_size = cnt;
            index_record = tv;
        }
    }
}

void init_join(){
    query_set.clear();
    for(int i = 1; i <= TABLE_LIMIT; i++){
        joined_table[i] = -1;
    }
    
    index_record.clear();
    table_num = 0;
}

int64_t join(const char * query){
    init_join();
    string query_s(query);
    int query_num = parse(query_s);
    for(int i = 0; i < query_num; i++){
        // cout << i << endl;
        join_one_query(query_set[i]);
    }

    int64_t sum = 0;
    for(int i = 0; i < index_record.size(); i++){
        for(int j = 0; j < table_num; j++){
            
                sum += index_record[i][j].second;
            
        }
    }
    
    
    return sum;
}
