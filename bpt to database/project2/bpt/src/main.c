#include "bpt.h"



int main(int argc, char ** argv){

	char input_file[MAX_LENGTH];

	int key, table_id, table_size;
	char value[MAX_LENGTH];
	char instruction;
	char * return_value;
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
				scanf("%d %d %s", &table_id, &key, value);
				insert(table_id, key, value);
				// print_tree();
				getchar();
				break;
			case 'f':
				scanf("%d %d", &table_id, &key);
				return_value = find(table_id, key);
				printf("%s\n", return_value);
				getchar();
				break;
			case 'o':
				scanf("%s", input_file);
				table_id = open_table(input_file);
				if(table_id < 0){
					printf("file open error\n");
				}
				printf("table id : %d\n", table_id);
				getchar();
				break;
			case 'd':
				scanf("%d %d", &table_id, &key);
				if(delete(table_id, key)){
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
				break;
		}
		printf("> ");
	}
	printf("\n");

	shutdown_db();

	return 0;
}