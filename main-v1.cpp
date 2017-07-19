/*********************************************************************** 
 * This code uses MPI library to count word frequencies distributed in *
 * several files. Manager-worker pattern is used for self-scheduling.  *
 * Author: Dawei Wang						       *
 * Date: 2017-07-03.						       *
 ***********************************************************************/

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <direct.h>
#include <Windows.h>

// Turn off warnings of unsafe strcpy and fopen in MS.
#pragma warning(disable:4996)

#define NUM_PROCESS 4

#define MAX_TASKS 100
#define MAX_WORD_COUNT 500
#define LINE_MAX_LEN 100
#define WORD_MAX_LEN 50
#define TOTAL_MAX_WORD_COUNT 5000

#define WORKER_RECV_TASK_STATUS 0
#define WORKER_READY_TO_SHUFFLE_STATUS 1
#define WORKER_READY_TO_GATHER_STATUS 2

#define MIN(x,y) (((x) < (y)) ? (x) : (y))

typedef struct {
	char word[WORD_MAX_LEN];
	int counts;
} word_count_pair;

int wordHash(char *word) {
	int hashcode = 0;
	int wordlen = strlen(word);
	for (int i = 0; i < wordlen; i++) {
		hashcode += (int) word[i];
	}
	hashcode %= NUM_PROCESS;
	return hashcode;
}

/* Read list of files to be processed from listFile, and return the 
   number of files in the list, refer to the following picture. */
int getFileList(FILE *listFile, char p[], char *filePaths[]) {
	int nlines = 0;
	char line[LINE_MAX_LEN];

	while (fgets(line, LINE_MAX_LEN, listFile) != NULL) {
		int len = strlen(line);
		if (len > 0) {
			line[len - 1] = '\0'; /* Delete newline */
			strcpy(p, line);
			filePaths[nlines++] = p;
			p += len;
		}
	}
	return nlines;
}

/**********************************************************************************
 *  A wrapper of the above function. Read data paths from listFile, and store     *
 *  each path in the continuous array content_array, with every starting          *
 *   address pointed to by pointers in p_array.                                   *
 *                       _________________________________________		  *
 *   content_array       |+++++++++|+++++++++|+++++++++|+++++++++|		  *
 *   Memory Addres       p1        p2        p3        p4			  *
 *                       _____________________					  *
 *   p_array             | p1 | p2 | p3 | p4 |					  *
 *										  *
 **********************************************************************************/
int getDataPaths(char listFile[], char content_array[], char *p_array[]) {
	int counts = 0;
	FILE *fp = fopen(listFile, "r");
	if (fp == NULL) {
		printf("Error: can't open list file!\n Errorno %d: (%s)\n", errno, strerror(errno));
		return -1;
	}
	else {
		counts = getFileList(fp, content_array, p_array);

		for (int i = 0; i < counts; i++) {
			printf("%s\n", p_array[i]);
		}
	}
	fclose(fp);

	return counts;
}

/* Generate the full path for creating a file named 
   fileName in the current app directory. */
char* genFullFilePath(char path[], const char fileName[]) {
	char pBuf[MAX_PATH];
	GetModuleFileName(NULL, pBuf, MAX_PATH); // Get Current App path.
	char drive[_MAX_DRIVE], dir[_MAX_DIR], fname[_MAX_FNAME], ext[_MAX_EXT];
	_splitpath_s(pBuf, drive, dir, fname, ext);
	//printf("%s\n", drive);
	//printf("%s\n", dir);

	sprintf(path, "%s%s%s", drive, dir, fileName);
	printf("%s\n", path);

	return path;
}

/* Almost the same as the above function, except that the fileName will
   be post-fixed with an index number. */
char *genFullIndexedFilePath(char path[], const char fileName[], int index) {
	char pBuf[MAX_PATH];
	GetModuleFileName(NULL, pBuf, MAX_PATH); // Get Current App path.
	char drive[_MAX_DRIVE], dir[_MAX_DIR], fname[_MAX_FNAME], ext[_MAX_EXT];
	_splitpath_s(pBuf, drive, dir, fname, ext);

	sprintf(path, "%s%s%s_%d", drive, dir, fileName, index);
	printf("%s\n", path);

	return path;
}

/* Print word count pair in structure array wcp[] of n elements to a 
   file pointed by fp, note here that fp can be stdout, so it can 
   also print to screen. */
void printWCPtoFile(FILE *fp, word_count_pair wcp[], int n) {
	fprintf(fp, "Total counts: %d\n", n);
	for (int i = 0; i < n; i++) {
		fprintf(fp, "word: %s\t", wcp[i].word);
		fprintf(fp, "count: %d\t\n", wcp[i].counts);
	}
	return;
}

/* Read a single word from file, if successful, return its first
   character, return EOF when reaching end of file */
int readWordFromFile(FILE *fp, char *word, int maxlen) {
	char *w = word;
	int c;
	while (isspace(c = getc(fp)))
		;
	if (!isspace(c)) {
		*w = c;
		w++;
		--maxlen;
	}

	for (; --maxlen > 0; w++) {
		if (!isalpha(*w = getc(fp))) {
			ungetc(*w, fp);
			break;
		}
	}
	*w = '\0';
	return word[0];
}

/* This can be seen as a wrapper of the above function. It read all the words
   from the file located in fullpath into word_count_pair structure array wcp[].
   All the words are initially appended with count 1. */
int readWordsToArray(word_count_pair wcp[], int *count_p, char fullpath[]){
	char word[WORD_MAX_LEN];
	FILE *fp = fopen(fullpath, "r");
	if (fp == NULL) {
		printf("Error: can't open file!\n Errorno %d: (%s)\n", errno, strerror(errno));
		return -1;
	}
	else {
		while (readWordFromFile(fp, word, WORD_MAX_LEN) != EOF) {
			strcpy(wcp[*count_p].word, word);
			wcp[*count_p].counts = 1;
			(*count_p)++;
		}
	}
	fclose(fp);
	return *count_p;
}

/* Combine two counted histogram, and the combined result would be stored in hist1. 
   Return the combined number of distinct words. */
int combineHistogram(word_count_pair dest[], int n1, word_count_pair source[], int n2) {
	int combined_count = n1;
	for (int j = 0; j < n2; j++) {
		bool gotmatch = false;
		for (int i = 0; i < combined_count; i++) {
			if (strcmp(dest[i].word, source[j].word) == 0) {
				dest[i].counts += source[j].counts;
				gotmatch = true;
				break;
			}
		}
		if (gotmatch) continue;
		strcpy(dest[combined_count].word, source[j].word);
		dest[combined_count].counts += source[j].counts;
		combined_count++;
	}
	return combined_count;
}

bool checkAllWorkerStatus(int *array, int status, int size) {
	for (int i = 0; i < size; i++) {
		if (array[i] != status)
			return false;
	}
	return true;
}

/* Add shuffling processors into an array maintained by manager.
   Return the number of processors which need shuffling in dest. */
int setTargetArray(int dest[], int n1, int source[], int n2) {
	int count = n1;

	for (int j = 0; j < n2; j++) {

		bool foundmatch = false;
		for (int i = 0; i < count; i++) {
			if (dest[i] == source[j]) {
				foundmatch = true;
				break;
			}
		}
		if (foundmatch) continue;
		dest[count] = source[j];
		count++;
	}
	return count;
}

/* Decide whether a processor with rank id is a target for shuffling. */
bool isTarget(int targArray[], int n, int id) {
	for (int i = 0; i < n; i++) {
		if (targArray[i] == id)
			return true;
	}
	return false;
}

int main(int argc, char *argv[]) {
	int proc_rank, numprocess, manager = 0, 
		tag_rawdata = 1,
		tag_requirework = 2, 
		tag_dispatchinfo = 3, 
		tag_shuffle = 4, 
		/* Note that we need to differentiate these two status tag, 
		   or there will be a deadlock caused by processor race!!! */
		tag_worker_status_change_1 = 5,
		tag_worker_status_change_2 = 6,
		tag_final = 7,
		tag_get_shuffle_procs = 8;

	word_count_pair wcp_s[MAX_WORD_COUNT], wcp_r[MAX_WORD_COUNT], 
		localcounted_array[MAX_WORD_COUNT];
	word_count_pair locallyRecvdPartialResult[MAX_WORD_COUNT];
	word_count_pair result[TOTAL_MAX_WORD_COUNT];

	memset(wcp_s, 0, MAX_WORD_COUNT*sizeof(word_count_pair));
	memset(wcp_r, 0, MAX_WORD_COUNT*sizeof(word_count_pair));
	memset(locallyRecvdPartialResult, 0, MAX_WORD_COUNT*sizeof(word_count_pair));
	memset(result, 0, MAX_WORD_COUNT*sizeof(word_count_pair));
	memset(localcounted_array, 0, MAX_WORD_COUNT*sizeof(word_count_pair));

	int num_localcountedwords = 0;
	int partialResultCounts = 0;
	int result_totalCounts = 0;
	int SentDone = 0;
	int WorkerStatus[NUM_PROCESS - 1] = {WORKER_RECV_TASK_STATUS};

	int num_targetProcess = 0;
	int shuffle_receive_process[NUM_PROCESS - 1];
	memset(shuffle_receive_process, 0, (NUM_PROCESS - 1 ) * sizeof(int));

	MPI_Datatype wordcount_t, oldtypes[2];
	int blockcounts[2];
	MPI_Aint offsets[2], extent;

	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocess);
	MPI_Comm_rank(MPI_COMM_WORLD, &proc_rank);
	printf("MPI processor %d has started ...\n", proc_rank);

	/* Define word-count pair derived data type */
	MPI_Type_extent(MPI_CHAR, &extent);
	offsets[0] = 0;
	oldtypes[0] = MPI_CHAR;
	blockcounts[0] = WORD_MAX_LEN;
	offsets[1] = WORD_MAX_LEN * extent;
	oldtypes[1] = MPI_INT;
	blockcounts[1] = 1;

	MPI_Type_struct(2, blockcounts, offsets, oldtypes, &wordcount_t);
	MPI_Type_commit(&wordcount_t);

	if (proc_rank == manager) {

		/* Prepration step: read file list */
		char fpaths_array[LINE_MAX_LEN * MAX_TASKS];
		char *fpaths_p[MAX_TASKS];
		int task_counts = 0;

		char path[1024];
		const char fileName[] = "data\\filelist.txt";
		genFullFilePath(path, fileName);

		task_counts = getDataPaths(path, fpaths_array, fpaths_p);

		/************************ Manager Part **************************/

		/* Read each file in the list and distribute file tasks */
		int task_sent = 0;
		for (int i = 0; i < MIN(NUM_PROCESS - 1, task_counts); i++) {
			int target_rank = i % (NUM_PROCESS - 1) + 1;

			char task_fullpath[1024];
			genFullFilePath(task_fullpath, fpaths_p[i]);
			
			int w_counts = 0;
			readWordsToArray(wcp_s, &w_counts, task_fullpath);

			MPI_Ssend(wcp_s, w_counts, wordcount_t, target_rank, tag_rawdata, MPI_COMM_WORLD);
			task_sent++;

			printf("First batch of task sending completed successfully!\n\n");
		}

		while (true) {
			printf("Listening on work requirement from workers ......\n");

			int signal;
			MPI_Status status_manager;
			MPI_Recv(&signal, 0, MPI_INT, MPI_ANY_SOURCE,
				tag_requirework, MPI_COMM_WORLD, &status_manager);
			int sender = status_manager.MPI_SOURCE;

			int HaveTask;
			if (task_sent < task_counts) {
				HaveTask = 1;
				MPI_Send(&HaveTask, 1, MPI_INT, sender, tag_dispatchinfo, MPI_COMM_WORLD);

				printf("Responded one requirement with task status signal!\n");

				char task_fullpath[1024];
				genFullFilePath(task_fullpath, fpaths_p[task_sent]);

				int w_counts = 0;
				readWordsToArray(wcp_s, &w_counts, task_fullpath);

				MPI_Send(wcp_s, w_counts, wordcount_t, sender, tag_rawdata, MPI_COMM_WORLD);
				task_sent++;

				printf("ONE SENT!\n\n");
			}
			else {
				printf("No task unsent in manager!\n");

				HaveTask = 0;
				MPI_Send(&HaveTask, 1, MPI_INT, sender, tag_dispatchinfo, MPI_COMM_WORLD);

				printf("Task dispatching complete signal sent to corresponding worker!\n\n");
				printf("Listening on worker status change signal!\n");
				int temp;
				MPI_Status status_temp;
				MPI_Recv(&temp, 1, MPI_INT, MPI_ANY_SOURCE, tag_worker_status_change_1, MPI_COMM_WORLD, &status_temp);
				int from_proc = status_temp.MPI_SOURCE;
				WorkerStatus[from_proc - 1] = temp;  // Worker ID starts from 1. BE CAREFULE!!!
				printf("Received status change signal from processor %d! Status changed to: %d\n", from_proc, temp);

				if (checkAllWorkerStatus(WorkerStatus, WORKER_READY_TO_SHUFFLE_STATUS, NUM_PROCESS - 1)) {
					printf("All workers are idle, ready to begin shuffle stage!\n\n");
					break;
				}
			}
		}

		/* Listening on the final status of all worker processors and set the 
		   corresponding element in the ReadyToGather array. */
		for (int i = 0; i < MIN(NUM_PROCESS - 1, task_counts); i++) {
			printf("Waiting for shuffle stage to be completed!\nListening on worker status change signal ......\n");
			int temp;
			MPI_Status status;
			MPI_Recv(&temp, 1, MPI_INT, MPI_ANY_SOURCE, tag_worker_status_change_2, 
				MPI_COMM_WORLD, &status);
			int from_proc = status.MPI_SOURCE;
			WorkerStatus[from_proc - 1] = temp;  // Worker ID starts from 1. BE CAREFULE!!!
			printf("Received status change signal from processor %d! Status changed to: %d\n\n", from_proc, temp);
		}

		/* Receiving processor ranks being involved in the shuffle stage,
		   and putting it into the maintained array. */
		for (int i = 0; i < MIN(NUM_PROCESS - 1, task_counts); i++) {
			printf("To Receive processor ids involved in shuffle stage ......\n");

			int recvd_array[NUM_PROCESS - 1] = { 0 };
			MPI_Status status;
			MPI_Recv(&recvd_array, NUM_PROCESS - 1, MPI_INT, MPI_ANY_SOURCE, tag_get_shuffle_procs,
				MPI_COMM_WORLD, &status);
			int from_proc = status.MPI_SOURCE;
			int num_recvd = 0;
			MPI_Get_count(&status, MPI_INT, &num_recvd);
			num_targetProcess = setTargetArray(shuffle_receive_process, num_targetProcess, recvd_array, num_recvd);

			printf("Received processor ids involved in shuffle stage from processor %d!\n", from_proc);
			printf("Recvd processor array are:\n");
			for (int i = 0; i < num_recvd; i++) {
				printf("%d\t", recvd_array[i]);
				if (i == num_recvd - 1)
					printf("\n\n");
			}
		}
		
		printf("Broadcasting shuffle processor ranks array to all workers!\n");
		MPI_Bcast(&num_targetProcess, 1, MPI_INT, manager, MPI_COMM_WORLD);
		MPI_Bcast(shuffle_receive_process, num_targetProcess, MPI_INT, manager, MPI_COMM_WORLD);
		printf("Broadcasting completed!\n\n");

		for (int i = 1; i <= num_targetProcess; i++) {
			printf("Begin sending final signal!\n");
			/* Although the corresponding Recv can listen on MPI_ANY_TAG and MPI_ANY_SOURCE,
			   the DataType in Send and Recv must match, so here we need to use wordcount_t. */
			MPI_Send(MPI_BOTTOM, 0, wordcount_t, i, tag_final, MPI_COMM_WORLD);
			printf("Manager final signal sent!\n");
		}

		int ready = checkAllWorkerStatus(WorkerStatus, WORKER_READY_TO_GATHER_STATUS, NUM_PROCESS - 1);
		if (ready) {
			MPI_Reduce(&partialResultCounts, &result_totalCounts, 1, MPI_INT, MPI_SUM, manager, MPI_COMM_WORLD);
			MPI_Gather(locallyRecvdPartialResult, partialResultCounts, wordcount_t, 
				result, partialResultCounts, wordcount_t, manager, MPI_COMM_WORLD);
		}
		printf("\nTotal counts: %d\n\n", result_totalCounts);
		//printWCPtoFile(stdout, result, result_totalCounts);
	}

	/****************************** Worker Part *******************************/

	else {
		/********************** Debug section *************************/

		int debug_fileindex = 0;

		/**************************************************************/

		while (true) {
			/* Count words locally (get local histogram). */
			MPI_Status status_worker;
			MPI_Recv(wcp_r, MAX_WORD_COUNT, wordcount_t, manager, 
				tag_rawdata, MPI_COMM_WORLD, &status_worker);
			int actualCountReceived = 0;
			/* Note that MPI_Status.count return the number of bytes received, so
			   it can't be used directly!!! BE CAREFUL!!! */
			MPI_Get_count(&status_worker, wordcount_t, &actualCountReceived);

			/************************ Debug section *****************************/
			char debug_filename[1024];
			char fileName1[] = "debug";
			genFullIndexedFilePath(debug_filename, fileName1, debug_fileindex);
			FILE *fpdebug = fopen(debug_filename, "a+");
			printWCPtoFile(fpdebug, wcp_r, actualCountReceived);
			fclose(fpdebug);
			/**********************************************************************/

			num_localcountedwords = combineHistogram(localcounted_array,
				num_localcountedwords, wcp_r, actualCountReceived);

			/************************ Debug section *****************************/
			char debug_localcount[1024];
			char fileName2[] = "debug_localcount";
			genFullIndexedFilePath(debug_localcount, fileName2, debug_fileindex);
			FILE *fp_countdebug = fopen(debug_localcount, "a+");
			printWCPtoFile(fp_countdebug, localcounted_array, num_localcountedwords);
			fclose(fp_countdebug);
			/**********************************************************************/

			/* Worker finished counting, require more task if still exist. */
			MPI_Send(MPI_BOTTOM, 0, MPI_INT, manager, tag_requirework, MPI_COMM_WORLD);

			int HaveWork;
			MPI_Recv(&HaveWork, 1, MPI_INT, manager, tag_dispatchinfo, 
				MPI_COMM_WORLD, MPI_STATUS_IGNORE);

			/************************ Debug section *****************************/
			char debug_dispatch[1024];
			char fileName3[] = "debug_dispatch";
			genFullIndexedFilePath(debug_dispatch, fileName3, debug_fileindex);
			FILE *fp_dispatchdebug = fopen(debug_dispatch, "a+");
			fprintf(fp_dispatchdebug, "HaveWork: %d\n", HaveWork);
			fclose(fp_dispatchdebug);
			/**********************************************************************/

			if (!HaveWork) {
				int ready_to_shuffle = WORKER_READY_TO_SHUFFLE_STATUS;
				MPI_Send(&ready_to_shuffle, 1, MPI_INT, manager, tag_worker_status_change_1, MPI_COMM_WORLD);

				/************************ Debug section *****************************/
				char debug_statuschange[1024];
				char fileName4[] = "debug_statuschange";
				genFullIndexedFilePath(debug_statuschange, fileName4, proc_rank);
				FILE *fp_statuschangedebug = fopen(debug_statuschange, "a+");
				fprintf(fp_statuschangedebug, "Processor %d status change signal sent (waiting to shuffle)!", proc_rank);
				fclose(fp_statuschangedebug);
				/**********************************************************************/

				break;
			}

			/****************** Debug Section ************************/

			debug_fileindex++;

			/*********************************************************/
		}

		/* Send items to corresponding processors (get hashed histogram). */
		MPI_Request request[MAX_WORD_COUNT];
		int req_index = 0;
		int targ_proc[NUM_PROCESS - 1] = { 0 };
		int num_tp = 0;
		for (int i = 0; i < num_localcountedwords; i++) {
			int hash_code = wordHash(localcounted_array[i].word);
			int target_proc = hash_code % (NUM_PROCESS - 1) + 1;

			if (target_proc != proc_rank) {
				int temp_array[1] = { target_proc };
				num_tp = setTargetArray(targ_proc, num_tp, temp_array, 1);

				/************************ Debug section *****************************/
				char debug_Issend[1024];
				char fname[] = "debug_Issend";
				genFullIndexedFilePath(debug_Issend, fname, i);
				FILE *fp_Issend = fopen(debug_Issend, "w");
				fprintf(fp_Issend, "Begin Issending word %s to processor %d.", localcounted_array[i].word, target_proc);
				fclose(fp_Issend);
				/**********************************************************************/

				MPI_Issend(localcounted_array + i, 1, wordcount_t, 
					target_proc, tag_shuffle, MPI_COMM_WORLD, &request[req_index]);
				req_index++;
			}
			else {
				strcpy(locallyRecvdPartialResult[partialResultCounts].word, 
					localcounted_array[i].word);
				locallyRecvdPartialResult[partialResultCounts].counts 
					= localcounted_array[i].counts;
				partialResultCounts++;
			}
		}


		int IMDONE = WORKER_READY_TO_GATHER_STATUS;
		MPI_Send(&IMDONE, 1, MPI_INT, manager, tag_worker_status_change_2, MPI_COMM_WORLD);
		/************************ Debug section *****************************/
		char debug_waitthensend[1024];
		char fileName6[] = "debug_waitthensend";
		genFullIndexedFilePath(debug_waitthensend, fileName6, proc_rank);
		FILE *fp_waitthensend = fopen(debug_waitthensend, "a+");
		fprintf(fp_waitthensend, "Processor %d waitall complete and notified the manager!", proc_rank);
		fclose(fp_waitthensend);
		/**********************************************************************/

		MPI_Send(targ_proc, num_tp, MPI_INT, manager, tag_get_shuffle_procs, MPI_COMM_WORLD);
		/************************ Debug section *****************************/
		char debug_target[1024];
		char fileName7[] = "debug_target";
		genFullIndexedFilePath(debug_target, fileName7, proc_rank);
		FILE *fp_target = fopen(debug_target, "a+");
		fprintf(fp_target, "Target sending processors of rank %d:\n", proc_rank);
		for (int i = 0; i < num_tp; i++) {
			fprintf(fp_target, "%d\t", targ_proc[i]);
		}
		fclose(fp_target);
		/**********************************************************************/

		MPI_Bcast(&num_targetProcess, 1, MPI_INT, manager, MPI_COMM_WORLD);
		MPI_Bcast(shuffle_receive_process, num_targetProcess, MPI_INT, manager, MPI_COMM_WORLD);
		/************************ Debug section *****************************/
		char debug_targranks[1024];
		char fileName8[] = "debug_targranks";
		genFullFilePath(debug_targranks, fileName8);
		FILE *fp_targranks = fopen(debug_targranks, "a+");
		fprintf(fp_targranks, "Target processor ranks are: \n");
		for (int i = 0; i < num_targetProcess; i++) {
			fprintf(fp_targranks, "%d\t", shuffle_receive_process[i]);
		}
		fclose(fp_targranks);
		/**********************************************************************/

		while (true) {
			if (isTarget(shuffle_receive_process, num_targetProcess, proc_rank)) {
				MPI_Status status_shuffle;
				//MPI_Recv(locallyRecvdPartialResult + partialResultCounts, 1, wordcount_t, 
					//MPI_ANY_SOURCE, tag_shuffle, MPI_COMM_WORLD, &status_shuffle);
				word_count_pair temp[1];
				memset(temp, 0, sizeof(word_count_pair));
				MPI_Recv(temp, 1, wordcount_t, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status_shuffle);
				if (status_shuffle.MPI_SOURCE == manager)
					break;
				partialResultCounts = combineHistogram(locallyRecvdPartialResult, partialResultCounts, temp, 1);
			}
			else 
				break;
		}

		MPI_Waitall(req_index, request, MPI_STATUS_IGNORE);

		/************************ Debug section *****************************/
		char debug_partialresult[1024];
		char fileName5[] = "debug_partialresult";
		genFullIndexedFilePath(debug_partialresult, fileName5, proc_rank);
		FILE *fp_presultdebug = fopen(debug_partialresult, "a+");
		printWCPtoFile(fp_presultdebug, locallyRecvdPartialResult, partialResultCounts);
		fclose(fp_presultdebug);
		/**********************************************************************/

		/* Gather locally distributed histograms to master processor */
		MPI_Reduce(&partialResultCounts, &result_totalCounts, 1, MPI_INT, MPI_SUM, manager, MPI_COMM_WORLD);
		MPI_Gather(locallyRecvdPartialResult, partialResultCounts, wordcount_t,
			result, partialResultCounts, wordcount_t, manager, MPI_COMM_WORLD);
	}

	MPI_Type_free(&wordcount_t);
	MPI_Finalize();

	return 0;
}
