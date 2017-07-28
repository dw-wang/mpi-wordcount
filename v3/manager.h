#ifndef MANAGER_H
#define MANAGER_H

#include <mpi.h>
#include <stdio.h>

#include "common.h"

typedef struct Manager {
	word_count_pair wcp_s[MAX_WORD_COUNT];
	word_count_pair result[TOTAL_MAX_WORD_COUNT];

	char *TaskPaths[MAX_TASKS];

	int task_counts;
	int task_sent;
	int result_totalCounts;
	int WorkerStatus[NUM_PROCESS - 1];

	int num_targetProcess;
	int shuffle_receive_process[NUM_PROCESS - 1];

	int num_shuffle_local;
	int num_shuffle_total;
} Manager;

void Init_Manager(Manager *manager) {
	manager->task_counts = 0;
	manager->task_sent = 0;
	manager->num_targetProcess = 0;
	manager->num_shuffle_local = 0;
	manager->num_shuffle_total = 0;

	memset(manager->WorkerStatus, WORKER_RECV_TASK_STATUS, (NUM_PROCESS - 1)*sizeof(int));
	memset(manager->wcp_s, 0, MAX_WORD_COUNT*sizeof(word_count_pair));
	memset(manager->result, 0, MAX_WORD_COUNT*sizeof(word_count_pair));
	memset(manager->shuffle_receive_process, 0, (NUM_PROCESS - 1)*sizeof(int));
}

/* Prepration step: read file list */
void GetTasks_Manager(Manager *manager, const char fileListName[]) {
	/* Prepration step: read file list */
	char fpaths_array[LINE_MAX_LEN * MAX_TASKS];
	char path[1024];
	genFullFilePath(path, fileListName);

	manager->task_counts = getDataPaths(path, fpaths_array, manager->TaskPaths);
}

void BcastTaskCounts_Manager(Manager *manager) {
	MPI_Bcast(&manager->task_counts, 1, MPI_INT, 0, MPI_COMM_WORLD);
}

/* Read each file in the list and distribute file tasks */
void DispatchTasks1_Manager(Manager *manager, MPI_Datatype wordcount_t) {
	/* Read each file in the list and distribute file tasks */
	for (int i = 0; i < MIN(NUM_PROCESS - 1, manager->task_counts); i++) {
		int target_rank = i % (NUM_PROCESS - 1) + 1;

		char task_fullpath[1024];
		genFullFilePath(task_fullpath, manager->TaskPaths[i]);

		int w_counts = 0;
		readWordsToArray(manager->wcp_s, &w_counts, task_fullpath);

		MPI_Send(manager->wcp_s, w_counts, wordcount_t, target_rank, tag_rawdata, MPI_COMM_WORLD);
		manager->task_sent++;

	}
	printf("First batch of task sending completed successfully!\n\n");
}

/* Listening on work requests from workers and respond accordingly. */
void RespondToWorkReq_Manager(Manager *manager, MPI_Datatype wordcount_t) {
	while (true) {
		printf("Listening on work requests from workers ......\n");

		int signal;
		MPI_Status status_manager;
		MPI_Recv(&signal, 0, MPI_INT, MPI_ANY_SOURCE,
			tag_requirework, MPI_COMM_WORLD, &status_manager);
		int sender = status_manager.MPI_SOURCE;

		int HaveTask;
		if (manager->task_sent < manager->task_counts) {
			HaveTask = 1;
			MPI_Send(&HaveTask, 1, MPI_INT, sender, tag_dispatchinfo, MPI_COMM_WORLD);

			printf("Responded one requirement with task status signal!\n");

			char task_fullpath[1024];
			genFullFilePath(task_fullpath, manager->TaskPaths[manager->task_sent]);

			int w_counts = 0;
			readWordsToArray(manager->wcp_s, &w_counts, task_fullpath);

			MPI_Send(manager->wcp_s, w_counts, wordcount_t, sender, tag_rawdata, MPI_COMM_WORLD);
			manager->task_sent++;

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
			MPI_Recv(&temp, 1, MPI_INT, MPI_ANY_SOURCE, tag_worker_status_change_1, 
				MPI_COMM_WORLD, &status_temp);
			int from_proc = status_temp.MPI_SOURCE;
			manager->WorkerStatus[from_proc - 1] = temp;  // Worker ID starts from 1. BE CAREFULE!!!
			printf("Received status change signal from processor %d! "
				   "Status changed to: %d\n", from_proc, temp);

			if (checkAllWorkerStatus(manager->WorkerStatus, WORKER_READY_TO_SHUFFLE_STATUS, 
				MIN(NUM_PROCESS - 1, manager->task_counts))) {
				printf("All workers are idle, ready to begin shuffle stage!\n\n");
				break;
			}
		}
	}
}

/* Listening on the final status of all worker processors and set the
   corresponding element in the ReadyToGather array. */
void CheckAllCompleteShuffle_Manager(Manager *manager) {
	for (int i = 0; i < MIN(NUM_PROCESS - 1, manager->task_counts); i++) {
		printf("Waiting for shuffle stage to be completed!\n"
			   "Listening on worker status change signal ......\n");
		int temp;
		MPI_Status status;
		MPI_Recv(&temp, 1, MPI_INT, MPI_ANY_SOURCE, tag_worker_status_change_2,
			MPI_COMM_WORLD, &status);
		int from_proc = status.MPI_SOURCE;
		manager->WorkerStatus[from_proc - 1] = temp;  // Worker ID starts from 1. BE CAREFULE!!!
		printf("Received status change signal from processor %d! Status changed to: %d\n\n", 
			from_proc, temp);
	}
}

/* Receiving processor ranks being involved in the shuffle stage,
   and putting it into the maintained array. */
// 这里肯定有错误，因为下面Bcast后打印出来的不对！可就是找不出来在哪！
// 更新：错误找出来了，原来确实是setTargetArray有错，里面的一个大括号丢了！
void BcastShuffleProcs_Manager(Manager *manager) {
	for (int i = 0; i < MIN(NUM_PROCESS - 1, manager->task_counts); i++) {
		printf("To Receive processor ids involved in shuffle stage ......\n");

		int recvd_array[NUM_PROCESS - 1] = { 0 };
		MPI_Status status;
		MPI_Recv(recvd_array, NUM_PROCESS - 1, MPI_INT, MPI_ANY_SOURCE, tag_get_shuffle_procs,
			MPI_COMM_WORLD, &status);
		int from_proc = status.MPI_SOURCE;
		int num_recvd = 0;
		MPI_Get_count(&status, MPI_INT, &num_recvd);
		printf("num recvd from processor %d is: %d\n", i, num_recvd);
		manager->num_targetProcess = setTargetArray(manager->shuffle_receive_process, 
			manager->num_targetProcess, recvd_array, num_recvd);

		printf("Received processor ids involved in shuffle stage from processor %d!\n", from_proc);
		printf("Recvd processor array are:\n");
		for (int j = 0; j < num_recvd; j++) {
			printf("%d\t", recvd_array[j]);
			if (j == num_recvd - 1)
				printf("\n\n");
		}
	}

	printf("Broadcasting shuffle processor ranks array to all workers!\n");
	MPI_Bcast(&manager->num_targetProcess, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(manager->shuffle_receive_process, manager->num_targetProcess, MPI_INT, 
		manager_rank, MPI_COMM_WORLD);
	printf("Broadcasting completed!\n\n");
}

/* In order to send a termination signal to all worker processors, the manager need to 
   know the total number of words involved in the shuffle stage. Using MPI_Reduce to get
   this number. */
void SumTotalNumOfWordsInShuffle_Manager(Manager *manager) {
	MPI_Reduce(&manager->num_shuffle_local, &manager->num_shuffle_total, 1, MPI_INT, 
		MPI_SUM, manager_rank, MPI_COMM_WORLD);
	printf("Number of words involved in shuffle: %d\n\n", manager->num_shuffle_total);
}

void RecordingShuffleWordsRecvd_Manager(Manager *manager) {
	for (int i = 0; i < manager->num_shuffle_total; i++) {
		MPI_Recv(MPI_BOTTOM, 0, MPI_INT, MPI_ANY_SOURCE, tag_term_sig, 
			MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	}
}

// 这个地方原先不对，因为对应到worker的代码，worker要先判断自己是不是target，是才会接收，
// 所以Manager要Send的数目是target process的数目，而不是MIN(NUM_PROCESS - 1, task_counts).
void SendTermSigToShuffleRecv_Manager(Manager *manager, MPI_Datatype wordcount_t) {
	for (int i = 1; i <= manager->num_targetProcess; i++) {
		printf("Begin sending final signal!\n");
		/* Although the corresponding Recv can listen on MPI_ANY_TAG and MPI_ANY_SOURCE,
		the DataType in Send and Recv must match, so here we need to use wordcount_t. */
		MPI_Ssend(MPI_BOTTOM, 0, wordcount_t, i, tag_final, MPI_COMM_WORLD);
		printf("Manager final signal sent!\n");
	}
}

void GetFinalResult_Manager(Manager *manager, MPI_Datatype wordcount_t) {
	int ready = checkAllWorkerStatus(manager->WorkerStatus, WORKER_READY_TO_GATHER_STATUS, 
		MIN(NUM_PROCESS - 1, manager->task_counts));
	if (ready) {
		printf("Haha, I entered the final stage and begin to receive the result!\n");
		MPI_Recv(&manager->result_totalCounts, 1, MPI_INT, MPI_ANY_SOURCE, 
			tag_gather_result, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		MPI_Recv(manager->result, manager->result_totalCounts, wordcount_t, 
			MPI_ANY_SOURCE, tag_gather_result, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	}
	printf("\nTotal counts: %d\n\n", manager->result_totalCounts);
	printWCPtoFile(stdout, manager->result, manager->result_totalCounts);
}

#endif /* MANAGER_H */