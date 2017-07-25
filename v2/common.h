#ifndef COMMON_H
#define COMMON_H

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <direct.h>

#ifdef _WIN32
#include <Windows.h>
#endif

// Turn off warnings of unsafe strcpy and fopen in Windows.
#ifdef _WIN32
#pragma warning(disable:4996)
#endif

#define manager_rank 0

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
		hashcode += (int)word[i];
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
*                       _________________________________________				  *
*   content_array       |+++++++++|+++++++++|+++++++++|+++++++++|				  *
*   Memory Addres       p1        p2        p3        p4						  *
*                       _____________________									  *
*   p_array             | p1 | p2 | p3 | p4 |									  *
*																				  *
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

#ifdef _WIN32
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

#elif __linux__


#endif

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
Return the number of processors which need shuffling into dest. */
int setTargetArray(int dest[], int n1, int source[], int n2) {
	int count = n1;

	for (int j = 0; j < n2; j++) {

		bool foundmatch = false;
		for (int i = 0; i < count; i++) {
			if (dest[i] == source[j]) 
			{
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


MPI_Datatype createWordCountPairType() {
	MPI_Datatype wordcount_t, oldtypes[2];
	int blockcounts[2];
	MPI_Aint offsets[2], extent;

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

	return wordcount_t;
}

/* Create a seperate communicator for workers from MPI_COMM_WORLD. */
void createWokerComm(MPI_Comm comm, MPI_Comm *worker_comm) {
	int ranks[1] = { 0 };
	MPI_Group world_group, worker_group;

	MPI_Comm_group(comm, &world_group);
	MPI_Group_excl(world_group, 1, ranks, &worker_group);
	MPI_Comm_create(comm, worker_group, worker_comm);
	MPI_Group_free(&world_group);
	MPI_Group_free(&worker_group);
}

#endif /* COMMON_H */