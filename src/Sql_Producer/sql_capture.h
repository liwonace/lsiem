#include <stdio.h>
#include <pcap.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include <pcap/pcap.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <locale.h>
#include <math.h>
#include "sys/socket.h"
#include <pthread.h>

#define PCAP_CNT_MAX 0
#define PCAP_SNAPSHOT 65536
#define PCAP_TIMEOUT 100
#define PCAP_FILTER "dst host 192.168.7.70"
#define ETH_ALEN 6                          
#define ETHERTYPE_IP 0x0800
#define DB_PORT 3306
#define DB_IP "192.168.7.70"
#define SOC_SERVER_IP "192.168.7.70"
#define SOC_SERVER_PORT 10470

struct ether_header{
	uint8_t ether_dhost[ETH_ALEN];
	uint8_t ether_shost[ETH_ALEN];
	uint16_t ether_type;
} __attribute__ ((__packed__));

struct packet_st{
	char packet[50000];
	int data_len;
	int port_dst;
	int port_src;
	char ip_src[20];
	char ip_dst[20];
	char bind_value[30000];
	char time[128];
	char query[50000];
	char host[128];
	char activename[128];
	char user[128];
	char program[128];
	char sid[128];
	char column[4096];
	char table_name[4096];
	char column_name[4096];
};

struct iptables{
	char *ip_list;
	int rej_flag;
};     

struct temp_st{	
	char ip_src[20];	
	char host[128];	
	char user[128];
	char program[128];
	char sid[128];
};

struct table_parsing_st{
	char name[4096];
	int start;
	int end;
	int check;
	int cp;
	int cp2;
	int cp3;
	int cp4;
	int cp5;
	int cp6;
};

struct packet_st pst_was[20];
struct packet_st pst_db[20];
struct temp_st tst[50];
struct iptables session[100];
struct table_parsing_st tname[10];

struct ip *iph;             
struct tcphdr *tcph;         

static pcap_t *pd;
static pcap_dumper_t *dumpfile;
static char *Device_Name;

int client;	
struct sockaddr_in serveraddr;
int flags = 0;
int data_len;
int port_src;
int port_dst;
char ip_src[20];
char ip_dst[20];
char currentTime[26];
char currentTime2[26];
int on_off = 0;
int st_count = 0;

void packet_view(unsigned char *, const struct pcap_pkthdr *, const unsigned char *); 
void result();
void query_parse(int data_len, char* packet, int port_dst);
void Data_Print(struct packet_st pst_db);