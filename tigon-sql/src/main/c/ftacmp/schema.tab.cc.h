typedef union {
	char* strval;
	field_entry *field_t;
	field_entry_list *field_list_t;
	table_def *table_def_t;
	table_list *table_list_t;
	param_list *plist_t;
	} YYSTYPE;
#define	NAME	258
#define	SEMICOLON	259
#define	LEFTBRACE	260
#define	RIGHTBRACE	261
#define	TABLE	262
#define	STREAM	263


extern YYSTYPE SchemaParserlval;
