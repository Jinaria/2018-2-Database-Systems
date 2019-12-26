#include "bpt.h"



int main(int argc, char ** argv){

	char input_file[MAX_LENGTH];
	
	int key;
	char value[MAX_LENGTH];
	char instruction;
	char * return_value;

	header = NULL;

	printf("> ");
	while(scanf("%c", &instruction) != EOF){
		switch(instruction){
			case 'i':
				scanf("%d %s", &key, value);
				insert(key, value);
				print_tree();
				getchar();
				break;
			case 'f':
				scanf("%d", &key);
				return_value = find(key);
				getchar();
				break;
			case 'o':
				scanf("%s", input_file);
				if(open_db(input_file)){
					printf("file open error\n");
				}
				getchar();
				break;
			case 'd':
				scanf("%d", &key);
				if(delete(key)){
					printf("delete fail\n");
				}
				print_tree();
				getchar();
				break;
		}
		printf("> ");
	}
	printf("\n");

	return 0;
}