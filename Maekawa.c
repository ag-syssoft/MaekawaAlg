/*
 * Maekawa.c
 *
 *  Created on: 18.06.2015
 *      Author: desiree
 */

#include <stdio.h>
#include <czmq.h>
#include <stdbool.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include "zhelpers.h"
#include "khash.h"
KHASH_MAP_INIT_INT(16, int)
khash_t(16) *h;
bool voted = false;
int candidateLamport = 0;
int candidateID;
int votes = 0;
int myStamp = 0;
char *state = "NONE";
int locked;
bool inquired = false;
bool selected = false;

int findProcessWithLowestTimeStamp(){
	int min = 1000;
	khiter_t k;
    for (k = kh_begin(h); k != kh_end(h); k++){
    	if (kh_exist(h, k)){
        	if(kh_value(h,k) < min){
        		min = kh_key(h, k);
        	}
        }
    }
    return min;
}

char* inquire(int procid, int id, int votingStamp){
	printf("Process: %d got inquire from %d \n", procid, id);
	if(strcmp(state, "FAILED") == 0 || myStamp > votingStamp){
		return "RELINGUISH.";
	}
	else{
		return "";
	}

}

char* request(int procid, int id, int votingStamp){
	printf("Process %d got request from %d at %d \n", procid, id, votingStamp );
	if(voted){
		int ret;
	    khiter_t k= kh_put(16, h, id, &ret);
		kh_value(h, k) = votingStamp;
		if(votingStamp < candidateLamport && !inquired){
			inquired = true;
			return "INQUIRE.";
		}
		else{
			return "FAIL.";
		}

	}
	else{
		candidateLamport = votingStamp;
		voted = true;
		locked = id;
		return "LOCKED.";
	}
}

char* release(int procid){
	printf("Process %d released critical action.\n", procid);
	inquired = false;
	state = "NONE";
	if(kh_size(h) == 0){
		voted = false;
		return "0, 0";
	}
	else{
		int process = findProcessWithLowestTimeStamp();
		int minTimeStamp = kh_value(h, process);
		kh_del(16, h, process);
		candidateID = process;
		candidateLamport = minTimeStamp;
		locked = candidateID;
		char back[30];
		sprintf(back, "%d, LOCKED.", candidateID);
		return strdup(back);
	}
}

char* relinguished(int procid){
	printf("Process: %d received relinguish.\n", procid);
	int ret;
	khiter_t k = kh_put(16, h, candidateID, &ret);
	kh_value(h, k) = candidateLamport;
	int process = findProcessWithLowestTimeStamp();
	int minTimeStamp = kh_value(h, process);
	kh_del(16, h, process);
	candidateID = process;
	candidateLamport = minTimeStamp;
	inquired = false;
	char back[30];
	sprintf(back, "%d, LOCKED.", candidateID);
	return strdup(back);

}

char* enterCriticalPart(int procid){
	printf("Process %d entered critical part.\n", procid);
	sleep(10);
	votes = 0;
	selected = false;
	kh_del(16, h, procid);
	fprintf(stderr, "Process %d left critical part.\n", procid);
	state = "RELEASED";
	return "RELEASE.";
}

char* prepareEntry(int procid){
	fprintf(stderr, "preparing Entry\n");
	printf("Process %d wants to enter", procid);
	selected = true;
	myStamp++;

	return "REQUEST.";
}

int main(int argc, char *argv[]){
	int procid = atoi(argv[2]);
	char *msg;
	char *recv_id;
	char *to;
	char msgSelected[255];
	int recvid = 0;
	char *recv_state;
	char *lamporttime;
	char *recv_msg;
	char send_msg[255];
	h = kh_init(16);
	void *context = zmq_ctx_new();
	void *publisher = zmq_socket(context, ZMQ_PUB);
	void *lamport_pub = zmq_socket(context, ZMQ_PUB);
	char routeraddr[30];
	int rc;
	int rc2;

	if(procid == 2){
		 rc = zmq_bind(publisher, "tcp://127.0.0.1:5555");
		 rc = zmq_bind(publisher, "tcp://127.0.0.1:5556");

		 rc2 = zmq_bind(lamport_pub, "tcp://127.0.0.1:5566");
		 rc2 = zmq_bind(lamport_pub, "tcp://127.0.0.1:5567");
		 rc2 = zmq_bind(lamport_pub, "tcp://127.0.0.1:5568");


	}

	if(procid == 3){
	 rc = zmq_bind(publisher, "tcp://127.0.0.1:5557");
	 rc = zmq_bind(publisher, "tcp://127.0.0.1:5558");

	 rc2 = zmq_bind(lamport_pub, "tcp://127.0.0.1:5569");
	 rc2 = zmq_bind(lamport_pub, "tcp://127.0.0.1:5570");
	 rc2 = zmq_bind(lamport_pub, "tcp://127.0.0.1:5571");

	}

	if(procid == 4){
		 rc = zmq_bind(publisher, "tcp://127.0.0.1:5559");
		 rc = zmq_bind(publisher, "tcp://127.0.0.1:5560");

		 rc2 = zmq_bind(lamport_pub, "tcp://127.0.0.1:5572");
		 rc2 = zmq_bind(lamport_pub, "tcp://127.0.0.1:5573");
		 rc2 = zmq_bind(lamport_pub, "tcp://127.0.0.1:5574");

	}

	if(procid == 5){
		 rc = zmq_bind(publisher, "tcp://127.0.0.1:5561");
		 rc = zmq_bind(publisher, "tcp://127.0.0.1:5562");

		 rc2 = zmq_bind(lamport_pub, "tcp://127.0.0.1:5575");
		 rc2 = zmq_bind(lamport_pub, "tcp://127.0.0.1:5576");
		 rc2 = zmq_bind(lamport_pub, "tcp://127.0.0.1:5577");


	}
	assert(rc == 0);
	assert(rc2 == 0);

	void *subscriber = zmq_socket(context, ZMQ_SUB);
	void *lamport_sub = zmq_socket(context, ZMQ_SUB);
	char addr[30];
    char *filter = (argc > 1)? argv [1]: "10001 ";
    char *filter2 = (argc > 1)? argv[1]: "10001" ;
    rc = zmq_setsockopt (subscriber, ZMQ_SUBSCRIBE, filter, strlen(filter));
    rc2 = zmq_setsockopt(lamport_sub, ZMQ_SUBSCRIBE, filter2, strlen(filter2));

    if(procid == 2){
     	 zmq_connect(subscriber, "tcp://127.0.0.1:5557");
      	 zmq_connect(subscriber, "tcp://127.0.0.1:5559");

      	 zmq_connect(lamport_sub, "tcp://127.0.0.1:5569");
      	 zmq_connect(lamport_sub, "tcp://127.0.0.1:5572");
      	 zmq_connect(lamport_sub, "tcp://127.0.0.1:5575");
    }

    if(procid == 3){
     	 zmq_connect(subscriber, "tcp://127.0.0.1:5555");
      	 zmq_connect(subscriber, "tcp://127.0.0.1:5561");

      	 zmq_connect(lamport_sub, "tcp://127.0.0.1:5566");
      	 zmq_connect(lamport_sub, "tcp://127.0.0.1:5573");
      	 zmq_connect(lamport_sub, "tcp://127.0.0.1:5576");

    }

    if(procid == 4){
     	 zmq_connect(subscriber, "tcp://127.0.0.1:5556");
      	 zmq_connect(subscriber, "tcp://127.0.0.1:5562");

      	 zmq_connect(lamport_sub, "tcp://127.0.0.1:5567");
      	 zmq_connect(lamport_sub, "tcp://127.0.0.1:5570");
      	 zmq_connect(lamport_sub, "tcp://127.0.0.1:5577");
    }

    if(procid == 5){
     	 zmq_connect(subscriber, "tcp://127.0.0.1:5560");
      	 zmq_connect(subscriber, "tcp://127.0.0.1:5558");

      	 zmq_connect(lamport_sub, "tcp://127.0.0.1:5568");
      	 zmq_connect(lamport_sub, "tcp://127.0.0.1:5571");
      	 zmq_connect(lamport_sub, "tcp://127.0.0.1:5574");
    }
    assert (rc == 0);

 int counter = 0;
 char *dummy;
 char *recv_lamport;
 char send_lamport[255];
 srand(time(NULL));

 while(1){
     recv_msg = s_recv_without_block(subscriber);
     recv_lamport = s_recv_without_block(lamport_sub);
     sleep(2);
     fprintf(stderr, "Received %s.\n", recv_msg);

     if(recv_lamport != NULL){
    	 myStamp = atoi(recv_lamport)+myStamp+1;
     }

     if(recv_msg != NULL){
      msg = strtok(recv_msg, ", ");
      dummy = msg;
      printf("Received %s \n", recv_msg);
      msg = strtok(NULL, ", ");
      lamporttime = msg;
      printf("lamporttime: %s \n", lamporttime);
      msg = strtok(NULL, ", ");
      recv_id = msg;
      printf("recv_id: %s \n", recv_id);
      msg = strtok(NULL, ", ");
      to = msg;
      printf("to: %s \n", to);
      msg = strtok(NULL, ".");
      recv_state = msg;
      printf("recv_state: %s \n", recv_state);

      if(strcmp(recv_state, " RELEASE") == 0 || strcmp(state, "RELEASED") == 0){
         sprintf(send_msg, "1, %s, %d, ", lamporttime, procid);
   	   strcat(send_msg, release(procid));
 			     printf ("\nPreparing to send data…\n");
 			     sleep(2);
 			     s_send(publisher, send_msg);
 	      printf("Data was send.\n");

       }

      if(strcmp(recv_state, " RELINGUISH") == 0 && atoi(to) == procid){
         sprintf(send_msg, "1, %s, %d, ", lamporttime, procid);
  	   strcat(send_msg, relinguished(procid));
	     printf ("\nPreparing to send data…\n");
	     sleep(2);
	  	     s_send(publisher, send_msg);
	     printf("Data was send.\n");
      }

      if(strcmp(recv_state, " LOCKED") == 0 && atoi(to) == procid ){
  	   votes++;
  	   if(votes == 3){
  		   state = "LOCKED";
  		   sprintf(send_msg, "1, %s, %d, all, ", lamporttime, procid);
  		   strcat(send_msg, enterCriticalPart(procid));

  		     printf ("\nPreparing to send data…\n");
  	  	     sleep(2);
  	  	  	     s_send(publisher, send_msg);
  	  	     printf("Data was send.\n");
  	   }

      }

      if(strcmp(recv_state, " REQUEST") == 0){
    	if(candidateLamport == 0){
         sprintf(send_msg, "1, %s, %d, %s, ", lamporttime, procid, recv_id);
    	}
    	else{
            sprintf(send_msg, "1, %d, %d, %s, ", candidateLamport, procid, recv_id);
    	}
  	   strcat(send_msg, request(procid, atoi(recv_id),
  		   atoi(lamporttime)));
	 	     printf ("\nPreparing to send data…\n");
	 	     sleep(2);
	 	     s_send(publisher, send_msg);
	     printf("Request was send.\n");
      }

      if(strcmp(recv_state, " INQUIRE") == 0 && atoi(to) == procid){
       sprintf(send_msg, "1, %s, %d, %s, ", lamporttime, procid, recv_id);
       char *inq = inquire(procid, atoi(recv_id), atoi(lamporttime) );
  	 strcat(send_msg, inq);
  	 if(strcmp(inq, "") == 0){
    	   votes++;
    	   if(votes == 3){
    		   state = "LOCKED";
    		   sprintf(send_msg, "1, %s, %d, all, ", lamporttime, procid);
    		   strcat(send_msg, enterCriticalPart(procid));
    		     printf ("\nPreparing to send data…\n");
    	  	     sleep(2);
    	  	  	     s_send(publisher, send_msg);
    	  	     printf("Data was send.\n");
    	   }
  	 }
  	 else{
	     printf ("\nPreparing to send data…\n");
	     sleep(2);
	     s_send(publisher, send_msg);
	     printf("Data was send.\n");
  	   }
      }
      if(strcmp(recv_state, " FAIL") == 0){
  	   voted--;
  	   state = "FAILED";
      }


     }
     fprintf(stderr, "%d\n", procid);
	   if(!selected && (rand()%20 == 0)){
		 voted = true;
		 votes++;
		 candidateLamport = myStamp;
		 candidateID = procid;
		 prepareEntry(procid);
		 int ret;
		 khiter_t k= kh_put(16, h, procid, &ret);
		 kh_value(h, k) = myStamp;
		 sprintf(send_lamport, "%i", myStamp);
		 sleep(2);
		 s_send(lamport_pub, send_lamport);
		 sprintf(send_msg, "1, %i, %i, all, ", myStamp, procid);
		 strcat(send_msg, "REQUEST.");
	     printf ("\nPreparing to send data…\n");
	     sleep(2);
         s_send(publisher, send_msg);
         printf("Data was send.\n");
	   }

  counter++;
 }
 return 0;

}

