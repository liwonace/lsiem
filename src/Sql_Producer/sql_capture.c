#include "sql_capture.h"

int main(int argc, char *argv[]) {
	Device_Name = "eno1";
	setlocale(LC_ALL, "");

    char errbuf[PCAP_ERRBUF_SIZE];
    bpf_u_int32 net;
    bpf_u_int32 netmask;
    struct in_addr net_addr, mask_addr;
	int ret = 0, i = 0, inum = 0;
	pcap_if_t *alldevs;
	pcap_if_t *d;
	struct bpf_program fp;
	
	ret = pcap_findalldevs(&alldevs, errbuf); 

	if(ret == -1){
		printf("pcap_findalldevs: %s\n", errbuf);
		exit(1);
	}

	for( d = alldevs; d; d = d->next){
		printf("%d: %s: ", ++i, d->name);

		if (d->description)
			printf("%d description: %s\n", i, d->description);
		else
			printf("No description available\n");
	}

    if(pcap_lookupnet(Device_Name, &net, &netmask, errbuf) < 0){
        perror(errbuf);
        exit(1);
    }

    net_addr.s_addr = net;
    mask_addr.s_addr = netmask;

    if((pd = pcap_open_live(Device_Name, PCAP_SNAPSHOT, 1, PCAP_TIMEOUT, errbuf)) == NULL){
        perror(errbuf);
        exit(1);
    }

    if(pcap_compile(pd, &fp, PCAP_FILTER, 0, net) < 0){
		printf("compile error\n");
		exit(1);
	}
		
	if(pcap_setfilter(pd, &fp) < 0){
		printf("setfilter error\n");
		exit(0);
	}
	
		      
	if((pcap_loop(pd, PCAP_CNT_MAX, packet_view, NULL) ) < 0){
		perror(pcap_geterr(pd));
		printf("dispatch error\n");
		exit(8);
	}               

	pcap_close(pd);
}

void packet_view(unsigned char *useless, const struct pcap_pkthdr *pkthdr, const unsigned char *packet){	
	struct timeval pc_time = pkthdr -> ts;
	gettimeofday(&pc_time,NULL);

	time_t rawtime1 = pc_time.tv_sec;
	struct tm * timeinfo1;
	
	char buffer1 [20];
	char buffer2 [20];
	
	time(&rawtime1);
	timeinfo1 = localtime(&rawtime1);
	
	strftime(buffer1, 20, "%Y%m%d", timeinfo1);
	
	char dumpfile_name[42];
	snprintf(currentTime, 26,  "%s", buffer1);
	snprintf(dumpfile_name, 42,  "./pcap_log/%s_pcap", buffer1);
	strftime(buffer2, 20, " %H:%M:%S", timeinfo1);
	
	snprintf(currentTime2, 26,  "%s.%d", buffer2,(int)pc_time.tv_usec);

	struct ether_header *ep;
	unsigned short ether_type;
	int length = pkthdr -> len;                       
	//unsigned char *back = packet;

	if(strcmp(Device_Name, "any") == 0){
		packet += 2;
	}
	
	ep = (struct ether_header *)packet;
	packet += sizeof(struct ether_header);
	ether_type = ntohs(ep->ether_type);
	
	unsigned int i, j;
	unsigned char byte;
	int k = 0;
	

	if(ether_type == ETHERTYPE_IP){
		
		iph = (struct ip *)packet;
	    
		if(iph->ip_p == IPPROTO_TCP){
			tcph = (struct tcp *)(packet + iph->ip_hl * 4);
			flags = tcph->psh;

			char temp_user[128];
			char temp_host[128];
			char temp_program[128];
			char temp_sid[128];
			
			memset(temp_user, '\0', 128);
			memset(temp_host, '\0', 128);
			memset(temp_program, '\0', 128);
			memset(temp_sid, '\0', 128);
			
			int host_start = 0, host_end = 0, host_check;
			int user_start = 0, user_end = 0, user_check;
			int program_start = 0, program_end = 0, program_check;
			int column_start = 0, column_end = 0, column_check;
			int sid_start = 0, sid_end = 0, sid_check;
			int packet_check = 0;
			int count = 0;
			int temp_packetI[20];
			
			data_len = ntohs(iph->ip_len); //- (iph->ip_hl*4) - (tcph->doff*4);
			int data_start = 40;
			
			/*data_len = ntohs(iph->ip_len) - (iph->ip_hl*4) - (tcph->doff*4);
			int data_start = (iph->ip_hl*4) + (tcph->doff*4);
			packet = packet + (iph->ip_hl*4) + (tcph->doff*4);*/

			strcpy(ip_src, inet_ntoa(iph->ip_src));
			strcpy(ip_dst, inet_ntoa(iph->ip_dst));
			port_src = ntohs(tcph->source);
			port_dst = ntohs(tcph->dest);
			/*printf("%d\n", ntohs(iph->ip_len));
			printf("%d\n", iph->ip_hl*4);
			printf("%d\n", tcph->doff*4);*/

			/*if(data_len > 10){
				for(i = 0;i < data_len+10;i++){
					printf("%c", packet[i]);
				}
				printf("\n%s\n",ip_src);
			}	*/		

			if(port_dst == DB_PORT && data_len > 10){
				printf("DB PORT");
				for(i = 0; i < data_len; i++){					
					pst_db[st_count].packet[i] = packet[i];
					//printf("%c", pst_db[st_count].packet[i]);
				}

				pst_db[st_count].packet[data_len] = '\0';				
				
				strcpy(pst_db[st_count].ip_src, ip_src);
				strcpy(pst_db[st_count].ip_dst, ip_dst);
				pst_db[st_count].data_len = data_len;
				pst_db[st_count].port_src = port_src;
				pst_db[st_count].port_dst = port_dst;
				result();				
			}			
		}	
	}
}

void result(){	
	int i;
					
	if(strlen(pst_db[st_count].packet) != 0){
		strcpy(pst_db[st_count].time, currentTime);
		strcat(pst_db[st_count].time, currentTime2);

		query_parse(pst_db[st_count].data_len, pst_db[st_count].packet, pst_db[st_count].port_dst);
		
		if(strlen(pst_db[st_count].query) != 0){
			printf("%s\n", pst_db[st_count].query);
			Data_Print(pst_db[st_count]);

			memset(pst_db[st_count].packet, '\0', 50000);
			memset(pst_db[st_count].ip_src, '\0', 20);
			memset(pst_db[st_count].ip_dst, '\0', 20);
			memset(pst_db[st_count].bind_value, '\0', 1024);
			memset(pst_db[st_count].time, '\0', 128);
			memset(pst_db[st_count].query, '\0', 50000);
			memset(pst_db[st_count].host, '\0', 128);
			memset(pst_db[st_count].activename, '\0', 128);
			memset(pst_db[st_count].user, '\0', 128);
			memset(pst_db[st_count].program, '\0', 128);
			memset(pst_db[st_count].sid, '\0', 128);
			memset(pst_db[st_count].table_name, '\0', 4096);
			memset(pst_db[st_count].column_name, '\0', 4096);
			pst_db[st_count].data_len = 0;
			pst_db[st_count].port_dst = 0;
			pst_db[st_count].port_src = 0;
			st_count++;

			if(st_count == 20)
				st_count = 0;					
		}
	}
}

void query_parse(int data_len, char* packet, int port_dst){
	int i, j, k, n, l;
	int count = 0;
	char temp_query[100000];
	char temp_activename[128];

	memset(temp_query, '\0', 100000);
	memset(temp_activename, '\0', 128);

	int query_start = 0, query_end = 0, query_check;
	int  bell;
	
	if(data_len > 10){
		/*--------------------------------- ALTER TABLE ---------------------------------*/
		if(strlen(temp_query) == 0){
			for(i = 0; i < data_len - 10; i++){
				if(packet[i] == 'A' && packet[i+1] == 'L' && packet[i+2] == 'T' && packet[i+3] == 'E' && packet[i+4] == 'R' && packet[i+5] == ' ' && packet[i+6] == 'T' && packet[i+7] == 'A' && packet[i+8] == 'B' && packet[i+9] == 'L' && packet[i+10] == 'E'){

					if(query_start == 0)
						query_start = i;
					
					query_check = 7;
					strcpy(temp_activename, "ALTER");
				}

				bell = packet[i];

				if(query_check == 7 && bell == 7){
					query_end = i;
				}
			}

			if(query_start != 0 && query_end > query_start){
				for(i = query_start; i < query_end; i++){
					temp_query[count] = packet[i];			
					count++;
				}
			}
			count = 0;
			query_check = 0;
		}

		if(strlen(temp_query) == 0){	
			for(i = 0; i < data_len - 10; i++){
				if(packet[i] == 'a' && packet[i+1] == 'l' && packet[i+2] == 't' && packet[i+3] == 'e' && packet[i+4] == 'r' && packet[i+5] == ' ' && packet[i+6] == 't' && packet[i+7] == 'a' && packet[i+8] == 'b' && packet[i+9] == 'l' && packet[i+10] == 'e'){
					if(query_start == 0)
						query_start = i;
					
					query_check = 7;
					strcpy(temp_activename, "alter");
				}

				bell = packet[i];

				if(query_check == 7 && bell == 7){
					query_end = i;
				}
			}

			if(query_end == 0){
				query_end = data_len;
			}

			if(query_start != 0 && query_end > query_start){
				for(i = query_start; i < query_end; i++){
					temp_query[count] = packet[i];			
					count++;
				}
			}

			count = 0;
			query_check = 0;
		}

		/*--------------------------------- TRUNCATE TABLE ---------------------------------*/
		/*if(strlen(temp_query) == 0){
			for(i = 0; i < data_len - 13; i++){
				if(packet[i] == 'T' && packet[i+1] == 'R' && packet[i+2] == 'U' && packet[i+3] == 'N' && packet[i+4] == 'C' && packet[i+5] == 'A' && packet[i+6] == 'T' && packet[i+7] == 'E' && packet[i+8] == ' ' && packet[i+9] == 'T' && packet[i+10] == 'A' && packet[i+11] == 'B' && packet[i+12] == 'L' && packet[i+13] == 'E'){
					if(query_start == 0)query_start = i;
					query_check = 7;
					strcpy(temp_activename,"TRUNCATE TABLE");
				}

				bell = packet[i];

				if(query_check == 7 && bell == 7){
					query_end = i;
				}
			}

			if(query_start != 0 && query_end > query_start){
				for(i = query_start; i < query_end; i++){
					temp_query[count] = packet[i];			
					count++;
				}
			}
			count = 0;
			query_check = 0;
		}

		if(strlen(temp_query) == 0){	
			for(i = 0; i < data_len - 13; i++){
				if(packet[i] == 't' && packet[i+1] == 'r' && packet[i+2] == 'u' && packet[i+3] == 'n' && packet[i+4] == 'c' && packet[i+5] == 'a' && packet[i+6] == 't' && packet[i+7] == 'e' && packet[i+8] == ' ' && packet[i+9] == 't' && packet[i+10] == 'a' && packet[i+11] == 'b' && packet[i+12] == 'l' && packet[i+13] == 'e'){
					if(query_start == 0)query_start = i;
					query_check = 7;
					strcpy(temp_activename,"TRUNCATE TABLE");
				}

				bell = packet[i];

				if(query_check == 7 && bell == 7){
					query_end = i;
				}
			}

			if(query_end == 0){
				query_end = data_len;
			}

			if(query_start != 0 && query_end > query_start){
				for(i = query_start; i < query_end; i++){
					temp_query[count] = packet[i];			
					count++;
				}
			}

			count = 0;
			query_check = 0;
		}*/

		/*--------------------------------- DROP TABLE ---------------------------------*/
		/*if(strlen(temp_query) == 0){
			for(i = 0; i < data_len - 9; i++){
				if(packet[i] == 'D' && packet[i+1] == 'R' && packet[i+2] == 'O' && packet[i+3] == 'P' && packet[i+4] == ' ' && packet[i+5] == 'T' && packet[i+6] == 'A' && packet[i+7] == 'B' && packet[i+8] == 'L' && packet[i+9] == 'E'){
					if(query_start == 0)query_start = i;
					query_check = 7;
					strcpy(temp_activename,"DROP TABLE");
				}

				bell = packet[i];

				if(query_check == 7 && bell == 7){
					query_end = i;
				}
			}

			if(query_start != 0 && query_end > query_start){
				for(i = query_start; i < query_end; i++){
					temp_query[count] = packet[i];			
					count++;
				}
			}
			count = 0;
			query_check = 0;
		}

		if(strlen(temp_query) == 0){		
			for(i = 0; i < data_len - 9; i++){
				if(packet[i] == 'd' && packet[i+1] == 'r' && packet[i+2] == 'o' && packet[i+3] == 'p' && packet[i+4] == ' ' && packet[i+5] == 't' && packet[i+6] == 'a' && packet[i+7] == 'b' && packet[i+8] == 'l' && packet[i+9] == 'e'){
					if(query_start == 0)query_start = i;
					query_check = 7;
					strcpy(temp_activename,"DROP TABLE");
				}

				bell = packet[i];

				if(query_check == 7 && bell == 7){
					query_end = i;
				}
			}

			if(query_end == 0){
				query_end = data_len;
			}

			if(query_start != 0 && query_end > query_start){
				for(i = query_start; i < query_end; i++){
					temp_query[count] = packet[i];			
					count++;
				}
			}

			count = 0;
			query_check = 0;
		}*/

		/*--------------------------------- BEGIN ---------------------------------*/
		/*if(strlen(temp_query) == 0){
			for(i = 0; i < data_len - 6; i++){
				if(packet[i] == 'B' && packet[i+1] == 'E' && packet[i+2] == 'G' && packet[i+3] == 'I' && packet[i+4] == 'N'){
					if(query_start == 0)query_start = i;
					query_check = 7;
					strcpy(temp_activename,"BEGIN");
				}

				bell = packet[i];

				if(query_check == 7 && bell == 7){
					query_end = i;
				}
			}

			if(query_start != 0 && query_end > query_start){
				for(i = query_start; i < query_end; i++){
					temp_query[count] = packet[i];			
					count++;
				}
			}
			count = 0;
			query_check = 0;
		}

		if(strlen(temp_query) == 0){			
			for(i = 0; i < data_len - 6; i++){
				if(packet[i] == 'b' && packet[i+1] == 'e' && packet[i+2] == 'g' && packet[i+3] == 'i' && packet[i+4] == 'n'){
					if(query_start == 0)query_start = i;
					query_check = 7;
					strcpy(temp_activename,"BEGIN");
				}

				bell = packet[i];

				if(query_check == 7 && bell == 7){
					query_end = i;
				}
			}

			if(query_end == 0){
				query_end = data_len;
			}

			if(query_start != 0 && query_end > query_start){
				for(i = query_start; i < query_end; i++){
					temp_query[count] = packet[i];			
					count++;
				}
			}

			count = 0;
			query_check = 0;
		}*/

		/*--------------------------------- INSERT ---------------------------------*/
		if(strlen(temp_query) == 0){
			for(i = 0; i < data_len - 7; i++){
				if(packet[i] == 'I' && packet[i+1] == 'N' && packet[i+2] == 'S' && packet[i+3] == 'E' && packet[i+4] == 'R' && packet[i+5] == 'T'){
					if(query_start == 0)
						query_start = i;
					
					query_check = 7;
					strcpy(temp_activename, "INSERT");
				}

				bell = packet[i];

				if(query_check == 7 && bell == 7){
					query_end = i;
				}
			}

			if(query_start != 0 && query_end > query_start){
				for(i = query_start; i < query_end; i++){
					temp_query[count] = packet[i];			
					count++;
				}
			}
			count = 0;
			query_check = 0;
		}

		if(strlen(temp_query) == 0){		
			for(i = 0; i < data_len - 7; i++){
				if(packet[i] == 'i' && packet[i+1] == 'n' && packet[i+2] == 's' && packet[i+3] == 'e' && packet[i+4] == 'r' && packet[i+5] == 't'){
					if(query_start == 0)
						query_start = i;
					
					query_check = 7;
					strcpy(temp_activename, "insert");
				}

				bell = packet[i];

				if(query_check == 7 && bell == 7){
					query_end = i;
				}
			}

			if(query_end == 0){
				query_end = data_len;
			}

			if(query_start != 0 && query_end > query_start){
				for(i = query_start; i < query_end; i++){
					temp_query[count] = packet[i];			
					count++;
				}
			}

			count = 0;
			query_check = 0;
		}

		/*--------------------------------- UPDATE ---------------------------------*/
		if(strlen(temp_query) == 0){
			for(i = 0; i < data_len - 7; i++){
				if(packet[i] == 'U' && packet[i+1] == 'P' && packet[i+2] == 'D' && packet[i+3] == 'A' && packet[i+4] == 'T' && packet[i+5] == 'E'){
					if(query_start == 0)
						query_start = i;
					
					query_check = 7;
					strcpy(temp_activename,"UPDATE");
				}

				bell = packet[i];

				if(query_check == 7 && bell == 7){
					query_end = i;
				}
			}

			if(query_end == 0){
				query_end = data_len;
			}

			if(query_start != 0 && query_end > query_start){
				for(i = query_start; i < query_end; i++){
					temp_query[count] = packet[i];			
					count++;
				}
			}

			count = 0;
			query_check = 0;
		}

		if(strlen(temp_query) == 0){
			for(i = 0; i < data_len - 7; i++){
				if(packet[i] == 'u' && packet[i+1] == 'p' && packet[i+2] == 'd' && packet[i+3] == 'a' && packet[i+4] == 't' && packet[i+5] == 'e'){
					if(query_start == 0)
						query_start = i;
					
					query_check = 7;
					strcpy(temp_activename,"update");
				}

				bell = packet[i];

				if(query_check == 7 && bell == 7){
					query_end = i;
				}
			}

			if(query_end == 0){
				query_end = data_len;
			}

			if(query_start != 0 && query_end > query_start){
				for(i = query_start; i < query_end; i++){
					temp_query[count] = packet[i];			
					count++;
				}
			}

			count = 0;
			query_check = 0;
		}

		/*--------------------------------- DELETE ---------------------------------*/
		if(strlen(temp_query) == 0){
			for(i = 0; i < data_len - 7; i++){
				if(packet[i] == 'D' && packet[i+1] == 'E' && packet[i+2] == 'L' && packet[i+3] == 'E' && packet[i+4] == 'T' && packet[i+5] == 'E'){
					if(query_start == 0)
						query_start = i;
					
					query_check = 7;
					strcpy(temp_activename,"DELETE");
				}

				bell = packet[i];

				if(query_check == 7 && bell == 7){
					query_end = i;
				}
			}

			if(query_end == 0){
				query_end = data_len;
			}

			if(query_start != 0 && query_end > query_start){
				for(i = query_start; i < query_end; i++){
					temp_query[count] = packet[i];			
					count++;
				}
			}

			count = 0;
			query_check = 0;
		}

		if(strlen(temp_query) == 0){
			for(i = 0; i < data_len - 7; i++){
				if(packet[i] == 'd' && packet[i+1] == 'e' && packet[i+2] == 'l' && packet[i+3] == 'e' && packet[i+4] == 't' && packet[i+5] == 'e'){
					if(query_start == 0)
						query_start = i;
					
					query_check = 7;
					strcpy(temp_activename,"delete");
				}

				bell = packet[i];

				if(query_check == 7 && bell == 7){
					query_end = i;
				}
			}

			if(query_end == 0){
				query_end = data_len;
			}

			if(query_start != 0 && query_end > query_start){
				for(i = query_start; i < query_end; i++){
					temp_query[count] = packet[i];			
					count++;
				}
			}

			count = 0;
			query_check = 0;
		}

		/*--------------------------------- SELECT ---------------------------------*/
		if(strlen(temp_query) == 0){
			for(i = 0; i < data_len - 7; i++){
				if(packet[i] == 's' && packet[i+1] == 'e' && packet[i+2] == 'l' && packet[i+3] == 'e' && packet[i+4] == 'c' && packet[i+5] == 't'){
					if(query_start == 0){
						query_start = i;
						//column_start = i + 7;
					}

					query_check = 7;
					strcpy(temp_activename,"select");
				}

				bell = packet[i];

				if(query_check == 7 && bell == 7){
					query_end = i;
				}
			}

			if(query_end == 0){
				query_end = data_len;
			}

			if(query_start != 0 && query_end > query_start){
				for(i = query_start; i < query_end; i++){
					temp_query[count] = packet[i];			
					count++;
				}
				printf("temp_query : %s\n", temp_query);
			}

			count = 0;
			query_check = 0;
		}

		if(strlen(temp_query) == 0){
			for(i = 0; i < data_len - 7; i++){
				if(packet[i] == 'S' && packet[i+1] == 'E' && packet[i+2] == 'L' && packet[i+3] == 'E' && packet[i+4] == 'C' && packet[i+5] == 'T'){
					if(query_start == 0){
						query_start = i;
						//column_start = i + 7;
					}
					
					query_check = 7;
					strcpy(temp_activename, "SELECT");
				}

				bell = packet[i];

				if(query_check == 7 && bell == 7){
					query_end = i;
				}
			}

			if(query_end == 0){
				query_end = data_len;
			}

			if(query_start != 0 && query_end > query_start){
				for(i = query_start; i < query_end; i++){
					temp_query[count] = packet[i];			
					count++;
				}
				//printf("temp_query : %s\n", temp_query);
			}
			count = 0;
			query_check = 0;
		}		
	}
	
	
	strcpy(pst_db[st_count].query, temp_query);
	strcpy(pst_db[st_count].activename, temp_activename);	

	memset(temp_query, '\0', 50000);
	memset(temp_activename, '\0', 128);
}

void Data_Print(struct packet_st pst_db){
	char set_data[50000];
	memset(set_data, '\0', 50000);

	if((client = socket(AF_INET, SOCK_STREAM, 0)) < 0){
		printf("Could not create socket : %d");
	}
	
	serveraddr.sin_family = AF_INET;
	serveraddr.sin_port = htons(SOC_SERVER_PORT);
	serveraddr.sin_addr.s_addr = inet_addr(SOC_SERVER_IP);	

	if(connect(client, (struct sockaddr *)&serveraddr, sizeof(serveraddr)) < 0){
        printf("can't connect.\n");
        exit(0);
    }
		
	sprintf(set_data, "%s~/%s~/%s~/%s~/%s~/", pst_db.time, pst_db.query, pst_db.ip_src, pst_db.ip_dst,pst_db.activename);
	printf("------------------------------------------------------\n");
	printf("TIME : %s\n", pst_db.time);
	printf("IP_SRC : %s\n", pst_db.ip_src);	
	printf("IP_DST : %s\n", pst_db.ip_dst);	
	printf("ACTIVE : %s\n", pst_db.activename);
	printf("QUERY : %s\n", pst_db.query);
	//printf("BIND : %s\n", pst_db.bind_value);
	printf("------------------------------------------------------\n\n");
	send(client, set_data, strlen(set_data), 0);

	close(client);
	memset(set_data, '\0', 50000);
		
}
