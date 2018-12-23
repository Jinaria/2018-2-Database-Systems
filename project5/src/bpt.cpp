#include "bpt.h"

buffer_manager bm;
int leaf_order = LEAF_ORDER;
int internal_order = INTERNAL_ORDER;
pagenum_t queue[100];
int start = 0, end = -1;

int fd[TABLE_LIMIT + 1];
int table_count = 0;
int next_table_num = 1;

table table_list[TABLE_LIMIT + 1];

pthread_mutex_t buf_man_lock;
trx_t trx_list[MAX_TRX_SIZE];
atomic<int> trx_count(1);



// Buffer Manager

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
    pthread_mutex_init(&buf_man_lock, NULL);

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
        bm.bf[i].pin_count++;
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
        if(bm.bf[bm.lru].is_dirty /*&& bm.bf[bm.lru].pin_count == 0*/){
            file_write_page(bm.bf[bm.lru].table_id, bm.bf[bm.lru].page_num, bm.bf[bm.lru].frame);
        }
        file_read_page(table_id, pn, &bm.bf[bm.lru].frame);
        bm.bf[bm.lru].table_id = table_id;
        bm.bf[bm.lru].page_num = pn;
        bm.bf[bm.lru].is_dirty = false;
        bm.bf[bm.lru].pin_count++;
       

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
        bm.bf[bm.buffer_num].pin_count++;
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
    
    
    if(buf == NULL)
        return NULL;
    while(!buf->frame->is_leaf){
        i = -1;
        if(key < buf->frame->internal[0].key){
            buf->pin_count--;
            buf = get_buffer(table_id, buf->frame->left_page_offset / PAGE_SIZE);
            
            continue;
        }
        for(i = buf->frame->num_of_keys - 1; i > -1; i--){
            if(key >= buf->frame->internal[i].key)
                break;
        }
        buf->pin_count--;
        buf = get_buffer(table_id, buf->frame->left_page_offset / PAGE_SIZE);
        
    }

    return buf;
}

int64_t * find(int table_id, int64_t key){
    int i = 0;
    buffer * buf;

    int64_t * value = new int64_t[VALUE_SIZE];
    buf = find_leaf(table_id, key);

    if(buf == NULL) return NULL;
    for(i = 0; i < buf->frame->num_of_keys; i++){
        if(buf->frame->leaf[i].key == key) break;
    }
    if(i == buf->frame->num_of_keys){
        printf("no key\n");
        buf->pin_count--;
        return NULL;
    }
    
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
        table_list[id].table_id = id;
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
        table_list[id].table_id = id;
        table_count++;

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
    root_buf->is_dirty = true;
    root_buf->pin_count--;
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
    
    leaf->is_dirty = true;
    leaf->pin_count--;

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

    new_leaf->is_dirty = true;
    
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
    right->pin_count--;
    

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
    buf->is_dirty = true;
    
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
    
    return delete_entry(table_id, buf, key);
    
}

void swap(int * a, int * b){
    int temp = *a;
    *a = *b;
    *b = temp;
}

int begin_tx(){
    int tid = trx_count.fetch_add(1);
    if(tid >= MAX_TRX_SIZE){
        printf("transaction list is full\n");
        return 0;
    }
    trx_list[tid].tid = tid;
    trx_list[tid].state = IDLE;


    return tid;
}

int end_tx(int tid){
    while(!trx_list[tid].trx_locks.empty()){
        pthread_rwlock_unlock(&(trx_list[tid].trx_locks.front()->lk));
        trx_list[tid].trx_locks.pop_front();
    }

    return tid;
}

int64_t * find(int table_id, int64_t key, int tid, int * result){
    int i = 0;
    buffer * buf;

    int64_t * value = new int64_t[VALUE_SIZE];

    pthread_mutex_lock(&buf_man_lock);
    buf = find_leaf(table_id, key);

    if(buf == NULL){
        pthread_mutex_unlock(&buf_man_lock);
        return NULL;   
    }
    
    auto temp = table_list[table_id].hash_page.find(buf->page_num);

    if(temp == table_list[table_id].hash_page.end()){
        lock_t t(table_id, buf->page_num, SHARED, &trx_list[tid]);
        
        trx_list[tid].wait_lock = &t;
        // deadlock detection
        pthread_rwlock_rdlock(&t.lk);
        trx_list[tid].trx_locks.push_back(&t);
        table_list[table_id].hash_page.insert({buf->page_num, t});
    }
    else{
        trx_list[tid].wait_lock = &temp->second;
        temp->second.trx = &trx_list[tid];
        temp->second.mode = SHARED;
        // deadlock detection
        pthread_rwlock_rdlock(&(temp->second.lk));
        trx_list[tid].trx_locks.push_back(&temp->second);
    }

    pthread_mutex_unlock(&buf_man_lock);   
    *result = 1;


    for(i = 0; i < buf->frame->num_of_keys; i++){
        if(buf->frame->leaf[i].key == key) break;
    }
    if(i == buf->frame->num_of_keys){
        printf("no key\n");
        buf->pin_count--;
        return NULL;
    }
    
    memcpy(value, buf->frame->leaf[i].value, sizeof(int64_t) * VALUE_SIZE);
    buf->pin_count--;


    return value;
}

int update(int table_id, int64_t key, int64_t * value, int tid, int * result){
    
    page * header = table_list[table_id].header;
    buffer * leaf;

    if(header == NULL){
        printf("Table is not opened\n");
        return -1;
    }

    pthread_mutex_lock(&buf_man_lock);

    leaf = find_leaf(table_id, key);
    if(leaf == NULL){ 
        pthread_mutex_unlock(&buf_man_lock);
        return -1;
    }

    auto temp = table_list[table_id].hash_page.find(leaf->page_num);

    if(temp == table_list[table_id].hash_page.end()){
        lock_t t(table_id, leaf->page_num, EXCLUSIVE, &trx_list[tid]);
        
        trx_list[tid].wait_lock = &t;
        // deadlock detection
        pthread_rwlock_wrlock(&t.lk);
        trx_list[tid].trx_locks.push_back(&t);
        table_list[table_id].hash_page.insert({leaf->page_num, t});
    }
    else{
        trx_list[tid].wait_lock = &temp->second;
        temp->second.trx = &trx_list[tid];
        temp->second.mode = EXCLUSIVE;
        // deadlock detection
        pthread_rwlock_wrlock(&(temp->second.lk));
        trx_list[tid].trx_locks.push_back(&temp->second);
    }

    pthread_mutex_unlock(&buf_man_lock);

    *result = 1;

    for(int i = 0; i < leaf->frame->num_of_keys; i++){
        if(leaf->frame->leaf[i].key == key){
            memcpy(leaf->frame->leaf[i].value, value, sizeof(int64_t) * VALUE_SIZE);

            return 0;
        }
    }

    
    return -1;
}