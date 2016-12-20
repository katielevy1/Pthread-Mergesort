/*
 * File: mergesortparallel.c
 * Author: Katie Levy and Michael Poyatt
 * Date: 10/25/16
 * Description: Parallel mergesort algorithm
 */

#include <pthread.h>
#include <math.h>
#include "mergesortparallel.h"

/* Defined in mersortparallel.h:
typedef struct Block {
    long start;
    long end;
} Block;
*/


/* Function Declarations */
void* threadFunc(void* rank);
int driverParallel(int start, int stop);
int mergeParallel(Block x, Block y, long tempStart, long tempEnd);
void printParallel(int start, int stop);
int validateParallel();
int binSearch(int arr[], int a, int b, int x);
void multiMerge(int numThreads, Block first, Block second, long tempStart, long tempEnd, long my_rank);


/* Global variables */
extern int threadCount;
extern long n; //array size
extern int *vecSerial;
extern int *vecParallel;
extern int *temp;
pthread_t *threads;
extern int threads_ready;
extern pthread_cond_t ready_cv;
extern pthread_mutex_t lock;



// MergeSort a global array in parallel.
int mergeSortParallel() {
    // Allocate memory for threads
    threads = malloc(threadCount*sizeof(pthread_t));
    // Create threads
    int i;
    for(i = 0; i < threadCount; i++){
        long rank = (long)i;
        pthread_create(&threads[i], NULL, threadFunc, (void*)rank);
    }
    // Join/terminate all the threads
    for(i = 0; i < threadCount; i++){
        pthread_join(threads[i], NULL);
    }
    free(threads);
    return 0;
}

// Function for each thread to perform.
// It mergesorts a portion of the array and mergesorts the rest using 
// a tree-reduction algorithm.
void* threadFunc(void* rank){
     // Get rank of this thread.
    long myRank = (long) rank;  /* Use long in case of 64-bit system */
    long level = 1;
    
    // Compute starting and ending indices for this thread to sum.
    long quotient = n / threadCount;
    long remainder = n % threadCount;
    long myCount;
    long myFirsti, myLasti;
    if (myRank < remainder) {
        myCount = quotient + 1;
        myFirsti = myRank * myCount;
    }
    else {
        myCount = quotient;
        myFirsti = myRank * myCount + remainder;
    }
    myLasti = myFirsti + myCount;
    printf("Thread: %ld before loop myFirstI %ld, mySecondI %ld myCount: %ld\n",  myRank, myFirsti, myLasti, myCount);

    if(myRank == 0){
        printParallel(0, n);

    }
    // Mergesort that starting subarray
    driverParallel(myFirsti, myLasti-1);

    int divisor = 2;
    int difference = 1;
    int rebase, indexAddOn;
    Block beginningRecur;
    Block beginningRecur2;

    // A barrier for all threads to be on the same level
        pthread_mutex_lock(&lock);
        threads_ready++;
        if(threads_ready == threadCount){
            printf("---------------BARRIER----------------\n");
            printParallel(0, n);
            for(int i = 0; i < threads_ready; i++){
                pthread_cond_signal(&ready_cv);
            }
            threads_ready = 0;
        }else {
            pthread_cond_wait(&ready_cv, &lock);
        }
        pthread_mutex_unlock(&lock);

    // Tree reduction to divide work amoung threads
    while(difference < threadCount) {

        quotient *= 2;
        myCount = quotient;
        rebase = myRank / divisor;
        myFirsti = rebase * myCount;
        myLasti = myFirsti + (myCount / 2);

        // Divide the remainder elements evenly amoungst the threads
        if(remainder != 0){
            // Check if a remainder value should be included
            if(((floor(myRank/(pow(2, level)))) * pow(2, level)) < remainder){
                int amount;
                // Check if remainder spread accross more than one team of threads
                if(remainder >= pow(2, level)){
                    myFirsti = -1;
                    // Check if each thread in team has remainder value
                    if((floor(myRank/(pow(2, level))) + 1) * pow(2, level) <= remainder){
                        amount = (int)pow(2, level);
                    }else{
                        amount = remainder / pow(2, level);
                        amount = amount % (int)(pow(2, level) + 1);
                        myFirsti = (rebase * myCount) + (remainder - amount);
                    }
                } else{
                    // When remainder is all in one team
                    amount = remainder % ((int)pow(2, level) + 1);
                }

                // Determine number of elements team is assigned to
                myCount = quotient + amount;
                if(myFirsti == -1){
                    myFirsti = rebase * myCount;
                }
                // Last i is dependent on how many remainder values
                if(level == 1){
                    myLasti = myFirsti + (quotient/2) + 1;
                }else{
                    myLasti = myFirsti + (quotient/2) + (amount % ((int)pow(2, level-1)+1));
                }
                printf("Thread %ld, myFirsti %ld, quotient %ld, amount %d, level %ld\n",
                    myRank, myFirsti, quotient, amount, level);
            } else{
                // When no remainder values are assigned to team
                // But have to add to index the remainder values in the lower numbered teams
                myFirsti += remainder;
                myLasti = myFirsti + (myCount / 2);
            }
        }
        printf("Thread %ld myCount %ld, level %ld, myr/pw(2,lev) %f, team %f\n", 
            myRank, myCount, level, floor(myRank/(pow(2, level))), ((floor(myRank/(pow(2, level)))) * pow(2, level)));

        
        beginningRecur.start = myFirsti;
        beginningRecur.end = myLasti;
        beginningRecur2.start = myLasti;
        beginningRecur2.end = myFirsti + myCount;
        printf("whileloop multimerge Thread %ld, divisor %d, first %ld - %ld, second %ld - %ld myCount: %ld\n", 
        myRank, divisor, beginningRecur.start, beginningRecur.end, beginningRecur2.start, beginningRecur2.end, myCount);
        
        // Call multiMerge to divide the two arrays by dividing them into x1, x2, y1, y2 until only one thread is assigned
        multiMerge(divisor, beginningRecur, beginningRecur2, beginningRecur.start, beginningRecur2.end, myRank);
        
        divisor *= 2;
        difference *=2;
        level++;

        // A barrier for all threads to be on the same level
        pthread_mutex_lock(&lock);
        threads_ready++;
        if(threads_ready == threadCount){
            printf("---------------BARRIER----------------\n");
            memcpy(vecParallel, temp, sizeof(int)*n);
            printParallel(0, n);
            for(int i = 0; i < threads_ready; i++){
                pthread_cond_signal(&ready_cv);
            }
            threads_ready = 0;
        }else {
            pthread_cond_wait(&ready_cv, &lock);
        }
        pthread_mutex_unlock(&lock);
    }
    return 0;
}

// Validates the sorting of the parallel algorithm
int validateParallel(){
    int i;
    for(i = 0; i < n-1; i++){
        if(vecParallel[i] != vecSerial[i]){
            printf("Parallel sort unsuccesful.\n");
            return 1;
        }
    }
    return 0;
}

// Recursively divides the array and merges
int driverParallel(int start, int stop){
    if(start >= stop){
        return 0;
    }
    int middle = ((stop + start) / 2);
    driverParallel(start, middle);
    driverParallel(middle+1, stop);
    Block first_merge;
    Block second_merge;
    first_merge.start = start;
    first_merge.end = middle + 1;
    second_merge.start = middle + 1;
    second_merge.end = stop + 1;
    mergeParallel(first_merge, second_merge, start, stop+1);
    int i;
    for(i = start; i <= stop; i++){
        vecParallel[i] = temp[i];
    }
    return 0;
}

// Merges two arrays
// Called by thread when only that thread is assigned to merge two subarrays
int mergeParallel(Block x, Block y, long tempStart, long tempEnd){
    // Fill array if one of the arrays is empty
    if(x.start == x.end){
        for(int i = 0; i < y.end - y.start; i++){
            temp[tempStart + i] = vecParallel[y.start + i];
        }
        return 0;
    } else if(y.start == y.end){
        for(int i = 0; i < x.end - x.start; i++){
            temp[tempStart + i] = vecParallel[x.start + i];
        }
        return 0;
    }
    int first = x.start;
    int second = y.start;
    int tempIndex = tempStart;
    while(first < x.end && second < y.end){
        printf("comparing %d and %d\n", vecParallel[first], vecParallel[second]);
        if(vecParallel[first] < vecParallel[second]){
            temp[tempIndex] = vecParallel[first];
            first++;
            tempIndex++;
        } else {
            temp[tempIndex] = vecParallel[second];
            second++;
            tempIndex++;
        }
    }
    while(first < x.end){
        temp[tempIndex] = vecParallel[first];
            first++;
            tempIndex++;
    }
    while(second < y.end){
        temp[tempIndex] = vecParallel[second];
            second++;
            tempIndex++;
    }
    return 0;
}

// Recursive binary search
int binSearch(int arr[], int a, int b, int x){
    if (b >= a){
        int mid = a + (b - a)/2;
        if (arr[mid] == x){
            return mid;}
        if(arr[mid] > x){
            return binSearch(arr, a, mid-1, x);}
        return binSearch(arr, mid+1, b, x);
    }
    return a;
}


// Prints the array used in parallel mergesort
void printParallel(int start, int stop){  
    int i;
    for(i = start; i < stop; i++){
        printf("%d\t", vecParallel[i]);
    }
    printf("\n");
    return;
}


// Divide subarray into x1, x2, y1, y2 for the threads assigned to that subarray. 
// If there is one thread, then call mergeParallel on the two subarrays
void multiMerge(int numThreads, Block first, Block second, long tempStart, long tempEnd, long my_rank){
    
    if (numThreads == 1){
        printf("Thread %ld merging first %ld - %ld and second %ld - %ld temp %ld - %ld\n", my_rank, first.start, first.end, second.start, second.end, tempStart, tempEnd);
        // Merge the two arrays
        mergeParallel(first, second, tempStart, tempEnd);
    } else {
        // Determine the indices for x1, x2, y1, and y2
        long x1start, x2start, x1end, x2end, x1size, x2size;
        long y1start, y2start, y1end, y2end, y1size, y2size, ymid;
        x1start = first.start;
        x2start = (first.start + first.end + 1) / 2;
        x1end = x2start;
        x2end = first.end;
        x1size = x1end - x1start;
        x2size = x2end - x2start;

        y1start = second.start;
        ymid = binSearch(vecParallel, second.start, second.end - 1, vecParallel[x2start]);
        if(ymid == second.end){
            y2start = second.end;
            y2end = second.end;
            y1end = second.end;
        }else if(ymid == second.start){
            y1start = second.start;
            y1end = second.start;
            y2start = ymid;
            y2end = second.end;
        } else {
            y2start = ymid;
            y1end = y2start;
            y2end = second.end;
        }
        y1size = y1end - y1start;
        y2size = y2end - y2start;

        // Split the threads in half and assign half the work
        numThreads /= 2;
        long divide = my_rank % (numThreads * 2);
        printf("Thread %ld of %d divide %ld \n\tx1start %ld, x2start %ld, x1end %ld, x2end %ld, x1size %ld x2size %ld \
        y1start %ld, y2start %ld, y1end %ld, y2end %ld, y1size %ld, y2size %ld\n\t temp %ld - %ld\n", 
        my_rank, numThreads * 2, divide, x1start, x2start, x1end, x2end, x1size, x2size,
        y1start, y2start, y1end, y2end, y1size, y2size, tempStart, tempEnd);
        if(my_rank % (numThreads * 2) < numThreads){
            // Assigned to work on x1 and y1
            Block fir;
            fir.start = x1start;
            fir.end = x1end;
            Block sec;
            sec.start = y1start;
            sec.end = y1end;
            tempEnd = tempStart + x1size + y1size;
            printf("Thread %ld of %d calling multimerge temp %ld - %ld\n", my_rank, numThreads, tempStart, tempEnd);
            multiMerge(numThreads, fir, sec, tempStart, tempEnd, my_rank);
        } else {
            // Assigned to work on x2 and y2
            Block fir; 
            fir.start = x2start;
            fir.end = x2end;
            Block sec;
            sec.start = y2start;
            sec.end = y2end;
            tempStart = tempStart + x1size + y1size;
            tempEnd = tempStart + x2size + y2size;
            printf("Thread %ld of %d calling multimerge temp %ld - %ld\n", my_rank, numThreads, tempStart, tempEnd);
            multiMerge(numThreads, fir, sec, tempStart, tempEnd, my_rank);
        }
    }
    return;
}
