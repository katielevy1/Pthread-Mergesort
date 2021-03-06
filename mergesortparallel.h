#include<stdlib.h>
#include<stdio.h>
#include<string.h>
#include<time.h>
#include<sys/time.h>
#include<pthread.h>


/* Shared Function Declaraions */
int mergeSortParallel();
int validateParallel();

typedef struct Block {
    long start;
    long end;
} Block;


/* Global variables */
extern int threadCount;
extern long n; //array size
extern int *vecSerial;
extern int *vecParallel;
extern int *temp;
extern int threads_ready;
extern pthread_cond_t ready_cv;
extern pthread_mutex_t lock;
void multiMerge(int numThreads, Block first, Block second, long tempStart, long tempEnd, long my_rank);

