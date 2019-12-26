
#include "bpt.h"



int main(int argc, char ** argv){

	char input_file[MAX_LENGTH];

	int64_t key;
	int table_id, table_size;
	int64_t value[VALUE_SIZE] = {0, };
	char instruction;
	int64_t * return_value;
	int64_t num;
	char s[200];
	int column;
	if(argc < 2)
		init_db(BUFFER_SIZE);
	else{
		table_size = atoi(argv[1]);
		init_db(table_size);
	}
	
	printf("> ");
	while(scanf("%c", &instruction) != EOF){
		switch(instruction){
			case 'i':
				cin >> table_id >> key;
				// scanf("%d %lld", &table_id, &key);
				for(int i = 0; i < 5; i++){
					cin >> value[i];
				}
				insert(table_id, key, value);
				// print_tree();
				getchar();
				break;
			case 'f':
				scanf("%d %lld", &table_id, &key);
				return_value = find(table_id, key);
				if(return_value != NULL){
					for(int i = 0; i < 15; i++)
						cout << return_value[i] << ' ';
					cout << endl;
				}
				getchar();
				break;
			case 'o':
				scanf("%s %d", input_file, &column);
				table_id = open_table(input_file, column);
				if(table_id < 0){
					printf("file open error\n");
				}
				else{
					printf("table id : %d\n", table_id);
				}
				getchar();
				break;
			case 'd':
				scanf("%d %lld", &table_id, &key);
				if(erase(table_id, key)){
					printf("delete fail\n");
				}
				// print_tree();
				getchar();
				break;
			case 'c':
				scanf("%d", &table_id);
				if(close_table(table_id)){
					printf("file close error\n");
				}
				getchar();
				break;
			case 's':
				if(shutdown_db()){
					printf("shutdown fail\n");
				}
				getchar();
				return 0;
			case 'p':
				cin >> table_id;
				print_tree(table_id);
				getchar();
				break;
		}
		printf("> ");
	}
	printf("\n");

	shutdown_db();

	return 0;
}