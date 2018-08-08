// Rob 08/08/18
//
// Capture output via pipe from Linux application and write to database
// Working Master
//
// Edit in server and mysql details
//
// ******************************************************************************* //
#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <my_global.h>
#include <mysql.h>
#include <string.h>
#include <stdlib.h>

//
//
// ******************************************************************************* //
//
#define DB_SERVER			"localhost"
#define DB_USER				"<USERID>"
#define DB_PASS				"<PASSWORD>"
#define DB_TABLE			"<TABLE NAME>"
#define DB_DATABASE 		"<DATABASE NAME>"
#define MAX_TOKENS			250
#define MAX_DBNAME_Size		64
#define LOG_MODE			0
//
#define	MAX_SQL_VAR_STR		1024					//Max length of variable seg of SQL command
#define SQLQUERY_PRE		"INSERT IGNORE INTO "	//SQL Command preamble
#define SQLQUERY_SFX		";\0"	//SQL Command tail inc NULL terminator
#define LBRACK				" ("
#define RBRACK				") "
#define EQUALS				" = "
#define COMMA				", "
#define VALUES				" VALUES "
#define DQUOTE				"\""
//
#define ATOM_MODE			0
#define OPEN_MODE			1
#define WRITE_MODE			2
#define CLOSE_MODE			3
#define DB_WRITE_MODE		0 // Set to 1 for bulk update mode.

// Global declares of SQL DB vars to support atomic and bulk modes
MYSQL *conn;
MYSQL_RES *res;
MYSQL_ROW row;
//
// ******************************************************************************* //
char *string_copy_inc(char *dest, const char *src)
{
	/* This is an adpation of the strcpy utility but it returns the next position 	*/
	/* in the destination string rather than the start (saves use of strlen)		*/
	/* Rob 14-04-18																	*/
		
	char *tmp = dest;

	while ((*dest++ = *src++) != '\0');

	char *end = dest; 	/* set end pointer */
	return --end;		/* decrement and return */
}
// ******************************************************************************* //

void hexDump(char *desc, void *addr, int len) 
{
    int i;
    unsigned char buff[17];
    unsigned char *pc = (unsigned char*)addr;

    // Output description if given.
    if (desc != NULL)
        printf ("%s:\n", desc);

    // Process every byte in the data.
    for (i = 0; i < len; i++) {
        // Multiple of 16 means new line (with line offset).

        if ((i % 16) == 0) {
            // Just don't print ASCII for the zeroth line.
            if (i != 0)
                printf("  %s\n", buff);

            // Output the offset.
            printf("  %04x ", i);
        }

        // Now the hex code for the specific character.
        printf(" %02x", pc[i]);

        // And store a printable ASCII character for later.
        if ((pc[i] < 0x20) || (pc[i] > 0x7e)) {
            buff[i % 16] = '.';
        } else {
            buff[i % 16] = pc[i];
        }

        buff[(i % 16) + 1] = '\0';
    }

    // Pad out last line if not exactly 16 characters.
    while ((i % 16) != 0) {
        printf("   ");
        i++;
}
    // And print the final ASCII bit.
    printf("  %s\n", buff);
}

// ******************************************************************************* //

int write_db(int mode,char *line_ptr,char *sql_server, char *sql_user, char *sql_pass,char *sql_db)
{
	char my_query_string[1024];
	char *server = sql_server;
	char *user = sql_user;
	char *password = sql_pass; /* set me first */
	char *database = sql_db;
	char *my_query = line_ptr;

	/* Connect to database */

	if ( (mode == OPEN_MODE)  || (mode == ATOM_MODE) ) 
	{
		
		conn = mysql_init(NULL);

		if (!mysql_real_connect(conn, server, user, password, database, 0, NULL, 0)) 
		{
			fprintf(stderr, "My command CONNECT failed: %s\n", mysql_error(conn));
			exit(1);
		}
	}

	if ( (mode == WRITE_MODE)  || (mode == ATOM_MODE) ) 
	{
		/* send SQL query */
		if (mysql_query(conn, my_query)) 
		{
			fprintf(stderr, "My command  %s %s\n", my_query, mysql_error(conn));
			exit(1);
		}
		res = mysql_use_result(conn);
	}

	if ( (mode == CLOSE_MODE)  || (mode == ATOM_MODE) )
	{
		/* close connection */
		mysql_free_result(res);
		mysql_close(conn);
	}
}

// ******************************************************************************* //
 int getSize(char* ch){
      int tmp=0;
      while (*ch) {
        *ch++;
        tmp++;
      }return tmp;}
	  
// ******************************************************************************* //
// CSV reader courtesy of http://ideone.com/mSCgPM with minor mods

int getcols( char *line, const char * const delim, char ***out_storage )
{
    char *start_ptr, *end_ptr, *iter;
    char **out;
    int i;                                          
    int tokens_found = 1, delim_size, line_size;    		//Calculate "line_size" indirectly, without strlen() call.
    int start_idx[MAX_TOKENS], end_idx[MAX_TOKENS]; 		//Store the indexes of tokens. Example "Power;": loc('P')=1, loc(';')=6
															//Change 100 with MAX_TOKENS or use malloc() for more than 100 tokens. Example: "b1;b2;b3;...;b200"

    if ( *out_storage != NULL )                 return -4;  //This SHOULD be NULL: Not Already Allocated
    if ( !line || !delim )                      return -1;  //NULL pointers Rejected Here
    if ( (delim_size = strlen( delim )) == 0 )  return -2;  //Delimiter not provided
	if ( delim_size != 1 ) 						return -8; 	//Invalid delim
    
	start_ptr = line;   
                        
	while ( ( end_ptr = strstr( start_ptr, delim ) ) ) {
        start_idx[ tokens_found -1 ] = start_ptr - line;    //Store the Index of current token
        end_idx[ tokens_found - 1 ] = end_ptr - line;       //Store Index of first character that will be replaced with
        tokens_found++;                                     //Accumulate the count of tokens.
        start_ptr = end_ptr + delim_size;                   //Set pointer to the next c-string within the line
    }
	
	if (tokens_found == 1) return 0;
    
	for ( iter = start_ptr; (*iter!='\0') ; iter++ );		//Locate the end position
	
    start_idx[ tokens_found -1 ] = start_ptr - line;    	//Store the Index of current token: of last token here.
    end_idx[ tokens_found -1 ] = iter - line;           	//and the last element that will be replaced with \0
    line_size = iter - line;    							//Saving CPU cycles: Indirectly Count the size of *line without using strlen();

    int size_ptr_region = (1 + tokens_found)*sizeof( char* );   		//The size to store pointers to c-strings + 1 (*NULL).
    out = (char**) malloc( size_ptr_region + ( line_size + 1 ) + 5 );   //Fit everything there...it is all memory.
    *out_storage = out;     											//Update the char** pointer of the caller function.


    for ( i = 0; i < tokens_found; i++ )    							//Assign adresses first part of the allocated memory pointers 
		out[ i ] = (char*) out + size_ptr_region + start_idx[ i ];  	//the second part of the memory, reserved for Data.
    
	out[ tokens_found ] = (char*) NULL; 					//[ ptr1, ptr2, ... , ptrN, (char*) NULL, ... ]: We just added the (char*) NULL.
															//Now assign the Data: c-strings. (\0 terminated strings):
    char *str_region = (char*) out + size_ptr_region;   	//Region inside allocated memory which contains the String Data.
    memcpy( str_region, line, line_size );   				//Copy input with delimiter characters: They will be replaced with \0.
	
	/* Replace: "arg1||arg2||arg3" with "arg1\0|arg2\0|arg3". Don't worry for characters after '\0'.  RB However - 			 	*/
    /* note this means you cannot operate directly on storage used here due to nulls.											*/

    for( i = 0; i < tokens_found; i++) str_region[ end_idx[ i ] ] = '\0';

    return tokens_found;
}
	
// ******************************************************************************* //

int main(int argc, char * argv[])
{
	/* get command line args */
	
	int c;
	int write_mode = DB_WRITE_MODE; /* set to compiled default */
	int log_mode = LOG_MODE;
	int tee_mode = 0;
	
	char db_database[MAX_DBNAME_Size] = { DB_DATABASE };
	char *db_database_ptr = db_database;

	char db_server[MAX_DBNAME_Size] = { DB_SERVER };
	char *db_server_ptr = db_server;

	char db_table[MAX_DBNAME_Size] = { DB_TABLE };
	char *db_table_ptr = db_table;

	char db_userid[MAX_DBNAME_Size] = { DB_USER };
	char *db_uid_ptr = db_userid;

	char db_dbp[MAX_DBNAME_Size] = { DB_PASS };
	char *db_dbp_ptr = db_dbp;

	const char * short_opt = "bd:hls:t:u:p:";
	static struct option long_opt[] =
	{
		{"bulk",no_argument,NULL,'b'},
		{"database",required_argument,NULL,'d'},
		{"help",no_argument,NULL,'h'},
		{"log",no_argument,NULL,'l'},
		{"server",required_argument,NULL,'s'},
		{"table",required_argument,NULL,'t'},
		{"user",required_argument,NULL,'u'},
		{"password",required_argument,NULL,'p'},
		{NULL,0,NULL,0}
	};
	
	while((c = getopt_long(argc, argv, short_opt, long_opt, NULL)) != -1)
	{
		switch(c)
		{
			case -1:		 /* no more arguments */
			case 0:		  /* long options toggles */
			break;

			case 'b':
				fprintf(stderr,"\nUsing bulk mode\n");
				write_mode = 1;
			break;
			
			case 'd':
				fprintf(stderr,"Using database named: \"%s\"\n", optarg);
				strcpy(db_database_ptr,optarg);
			break;

			case 'h':
				printf("Usage: %s [OPTIONS]\n", argv[0]);
				printf("  -b, --bulk	for bulk mode (leaves DB open for session)\n");
				printf("  -d <name>, --database <name>	selects specific database (default is %s)\n",DB_TABLE);
				printf("  -h, --help	print this help and exit\n");
				printf("  -l, --log		for log mode (creates second output stream to stdout)\n");
				printf("  -s <name>, --server <name>	selects specific database server (default is %s)\n",DB_TABLE);
				printf("  -t <name>, --table <name>	selects specific database table (default is %s)\n",DB_TABLE);
				printf("  -u <name>, --user <name>	selects specific database user id (default is %s)\n",DB_TABLE);
				printf("  -p <name>, --password <name>	selects specific database password (default is %s)\n",DB_TABLE);
				printf("\n");
			return(0);

			case 'l':
				fprintf(stderr,"\nUsing log mode (second output to stdout)\n");
				log_mode = 1;
			break;

			case 's':
				fprintf(stderr,"Using database server named: \"%s\"\n", optarg);
				strcpy(db_server_ptr,optarg);
			break;

			case 't':
				fprintf(stderr,"Using database table named: \"%s\"\n", optarg);
				strcpy(db_table_ptr,optarg);
			break;

			case 'u':
				fprintf(stderr,"Using DB UID named: \"%s\"\n", optarg);
				strcpy(db_uid_ptr,optarg);
			break;

			case 'p':
				fprintf(stderr,"Using DB Password named: \"%s\"\n", optarg);
				strcpy(db_dbp_ptr,optarg);
			break;

			case ':':
			case '?':
				fprintf(stderr, "Try `%s --help' for more information.\n", argv[0]);
			return(-2);

			default:
				fprintf(stderr, "%s: invalid option -- %c\n", argv[0], c);
				fprintf(stderr, "Try `%s --help' for more information.\n", argv[0]);
			return(-2);
		};
	};
	
	/*
	
	Current wh1080 csv data fields:
	time,model,id,channel,battery,temperature_C,crc,rid,button,humidity,state,rain_rate,rain_total,unit,group_call,command,dim,dim_value,wind_speed,wind_gust,wind_direction,device,temperature_F,direction_str,direction_deg,speed,gust,rain,msg_type,hours,minutes,seconds,year,month,day,ws_id,rainfall_mm,wind_speed_ms,gust_speed_ms,rc,flags,maybetemp,binding_countdown,depth,power0,power1,power2,node,ct1,ct2,ct3,ct4,Vrms/batt,temp1_C,temp2_C,temp3_C,temp4_C,temp5_C,temp6_C,pulse,sid,transmit,moisture,status,type,make,pressure_PSI,battery_mV,checksum,pressure,code,power,device id,len,to,from,payload,heartbeat,time_unix,binary";

	My superset are:
	

	1,2,3,5,6,10,15,38,39,40,41,42
	time,model,id,,battery,temperature_C,,,,humidity,,,,,,,,,,,,,,,,,,wind_speed,wind_gust,wind_direction,device,temperature_F,direction_str,direction_deg,speed,gust,rain,msg_type	
	
	*/
	
	
	char *sql_labels[] = /* DB fields  - must be sorted as per CSV sequence below */
	{
		"time",
		"model",
		"id",
		"battery",
		"temperature_C",
		"humidity",
		"direction_str",
		"direction_deg",
		"speed",
		"gust",
		"rain",
		"msg_type"
	};

	int required_fields[] = /* CSV fields required - must map to labels by position */
	{
		1,2,3,5,6,10,15,38,39,40,41,42
	};

	char sql_command_var[MAX_SQL_VAR_STR];
	char *sql_command_var_ptr;
	
	char line[BUFSIZ];
	char *p_line = line;
	
	int len;
	unsigned long int num_lines = 0;
    char delim[] = ",";
	int total_fields = sizeof(required_fields)/sizeof(required_fields[0]);
	
	sql_command_var_ptr = sql_command_var;

	if (write_mode == 1) write_db(OPEN_MODE,sql_command_var_ptr,db_server_ptr,db_uid_ptr,db_dbp_ptr,db_database_ptr);  // Open database in bulk mode
	
	while ( fgets(line,BUFSIZ,stdin) != NULL )
	{
		char **columns;
		int i,j;
		int current_column,current_field;

		num_lines++;
		
		if (log_mode = 1) fprintf(stdout,p_line);
		
		columns = NULL; //Should be NULL to indicate that it is not assigned to allocated memory. Otherwise return -4;
		int cols_found = getcols( p_line, delim, &columns);
		
		if (cols_found > 0) 
		{
				
			// Build the command prefix
			
			sql_command_var_ptr = sql_command_var;

			sql_command_var_ptr = string_copy_inc(sql_command_var_ptr,SQLQUERY_PRE);
			sql_command_var_ptr = string_copy_inc(sql_command_var_ptr,db_table);
			sql_command_var_ptr = string_copy_inc(sql_command_var_ptr,LBRACK);

			// Build the cols list
			
			for ( i = 0; i <  total_fields; i++ )
				{
					if (i != 0) {
						sql_command_var_ptr = string_copy_inc(sql_command_var_ptr,COMMA);
					}
					
					current_field = required_fields[i];
					current_column = current_field - 1;

					sql_command_var_ptr = string_copy_inc(sql_command_var_ptr,sql_labels[i]);
			
				} // end of looping through columns

				// ADD Centre of string
				
				sql_command_var_ptr = string_copy_inc(sql_command_var_ptr,RBRACK);
				sql_command_var_ptr = string_copy_inc(sql_command_var_ptr,VALUES);
				sql_command_var_ptr = string_copy_inc(sql_command_var_ptr,LBRACK);

				// Fill values
				
				for ( i = 0; i <  total_fields; i++ )
				{
					
					if (i != 0) {
						sql_command_var_ptr = string_copy_inc(sql_command_var_ptr,COMMA);
					}
					
					current_field = required_fields[i];
					current_column = current_field - 1;

					sql_command_var_ptr = string_copy_inc(sql_command_var_ptr,DQUOTE);
					sql_command_var_ptr = string_copy_inc(sql_command_var_ptr,columns[current_column]);
					sql_command_var_ptr = string_copy_inc(sql_command_var_ptr,DQUOTE);
					
				} // end of looping through columns


			sql_command_var_ptr = string_copy_inc(sql_command_var_ptr,RBRACK);
			sql_command_var_ptr = string_copy_inc(sql_command_var_ptr,SQLQUERY_SFX);

			free( columns );    //Release the Single Contiguous Memory Space.
			columns = NULL;     //Pointer = NULL to indicate it does not reserve space and that is ready for the next malloc().

			sql_command_var_ptr = sql_command_var;

			if (write_mode == 0) 
				write_db(ATOM_MODE,sql_command_var_ptr,db_server_ptr,db_uid_ptr,db_dbp_ptr,db_database_ptr);
			else
				write_db(WRITE_MODE,sql_command_var_ptr,db_server_ptr,db_uid_ptr,db_dbp_ptr,db_database_ptr);
			
			fprintf(stderr,"Lines read and written: %i\r",num_lines);
			}
		}
	if (write_mode == 1) write_db(CLOSE_MODE,sql_command_var_ptr,db_server_ptr,db_uid_ptr,db_dbp_ptr,db_database_ptr);
	fprintf(stderr,"\n");

}
