#ifndef WORKER_H
#define WORKER_H

#include <mpi.h>
#include <stdio.h>

#include "common.h"

typedef struct Worker {
	word_count_pair wcp_r[MAX_WORD_COUNT], localcounted_array[MAX_WORD_COUNT];
	word_count_pair locallyRecvdPartialResult[MAX_WORD_COUNT];
	word_count_pair result[TOTAL_MAX_WORD_COUNT];

	int task_counts;
	int num_localcountedwords;
	int partialResultCounts;
	int num_targetProcess;
	int result_totalCounts;

	int partialResultCounts_array[NUM_PROCESS - 1];
	int shuffle_receive_process[NUM_PROCESS - 1];

	int num_shuffle_local;
	int num_shuffle_total;

	MPI_Request request[MAX_WORD_COUNT];
} Worker;

void Init_Worker(Worker *worker) {
	worker->task_counts = 0;
	worker->num_localcountedwords = 0;
	worker->num_shuffle_local = 0;
	worker->num_shuffle_total = 0;
	worker->num_targetProcess = 0;
	worker->result_totalCounts = 0;
	worker->partialResultCounts = 0;

	memset(worker->shuffle_receive_process, 0, (NUM_PROCESS - 1)*sizeof(int));
	memset(worker->wcp_r, 0, MAX_WORD_COUNT*sizeof(word_count_pair));
	memset(worker->locallyRecvdPartialResult, 0, MAX_WORD_COUNT*sizeof(word_count_pair));
	memset(worker->localcounted_array, 0, MAX_WORD_COUNT*sizeof(word_count_pair));
	memset(worker->result, 0, MAX_WORD_COUNT*sizeof(word_count_pair));
}

void BcastTaskCounts_Worker(Worker *worker) {
	MPI_Bcast(&(worker->task_counts), 1, MPI_INT, manager_rank, MPI_COMM_WORLD);
}

void RecvAndProcessTasks_Worker(Worker *worker, MPI_Datatype wordcount_t) {
	/* Count words locally (get local histogram). */
	MPI_Status status_worker;
	MPI_Recv(worker->wcp_r, MAX_WORD_COUNT, wordcount_t, manager_rank,
		tag_rawdata, MPI_COMM_WORLD, &status_worker);

	int actualCountReceived = 0;
	/* Note that MPI_Status.count return the number of bytes received, so
	it can't be used directly!!! BE CAREFUL!!! */
	MPI_Get_count(&status_worker, wordcount_t, &actualCountReceived);

	worker->num_localcountedwords = combineHistogram(worker->localcounted_array,
		worker->num_localcountedwords, worker->wcp_r, actualCountReceived);

	/* Worker finished counting, require more task if still exist. */
	MPI_Send(MPI_BOTTOM, 0, MPI_INT, manager_rank, tag_requirework, MPI_COMM_WORLD);
}

int RequireWork_Worker() {
	int HaveWork;
	MPI_Recv(&HaveWork, 1, MPI_INT, manager_rank, tag_dispatchinfo,
		MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	if (HaveWork) {
		return 1;
	}
	else {
		int ready_to_shuffle = WORKER_READY_TO_SHUFFLE_STATUS;
		MPI_Send(&ready_to_shuffle, 1, MPI_INT, manager_rank, 
			tag_worker_status_change_1, MPI_COMM_WORLD);

		return 0;
	}
}

void ShuffleIssend_Worker(Worker *worker, int proc_rank, MPI_Datatype wordcount_t) {
	/* Send items to corresponding processors (get hashed histogram). */
	int req_index = 0;
	int targ_proc[NUM_PROCESS - 1] = { 0 };
	int num_tp = 0;

	for (int i = 0; i < worker->num_localcountedwords; i++) {
		int hash_code = wordHash(worker->localcounted_array[i].word);
		int target_proc = hash_code % (NUM_PROCESS - 1) + 1;

		if (target_proc != proc_rank) {
			int temp_array[1] = { target_proc };
			num_tp = setTargetArray(targ_proc, num_tp, temp_array, 1);

			MPI_Issend(worker->localcounted_array + i, 1, wordcount_t,
				target_proc, tag_shuffle, MPI_COMM_WORLD, &worker->request[req_index]);
			req_index++;
		}
		else {
			strcpy(worker->locallyRecvdPartialResult[worker->partialResultCounts].word,
				worker->localcounted_array[i].word);
			worker->locallyRecvdPartialResult[worker->partialResultCounts].counts
				= worker->localcounted_array[i].counts;
			(worker->partialResultCounts)++;
		}
	}

	int IMDONE = WORKER_READY_TO_GATHER_STATUS;
	MPI_Send(&IMDONE, 1, MPI_INT, manager_rank, tag_worker_status_change_2, MPI_COMM_WORLD);

	// 其实可以考虑采用MPI_Allgatherv将targ_proc收集起来，但是这样的话收集起来的target_array
	// 会有很多duplicate，最糟糕的情况是(NUM_PROCESS - 1)^2阶，如果收集之前不去重的话情况更糟，
	// 而且收集之后还是得去重，因为下面要用到总数，而且下面用isTarget()查询的时候会很慢，具体这里还有待商榷。
	// 注意这里出过错！！！
	MPI_Send(targ_proc, num_tp, MPI_INT, manager_rank, tag_get_shuffle_procs, MPI_COMM_WORLD);

	worker->num_shuffle_local = req_index;
}

void BcastShuffleProcs_Worker(Worker *worker) {
	MPI_Bcast(&worker->num_targetProcess, 1, MPI_INT, manager_rank, MPI_COMM_WORLD);
	MPI_Bcast(worker->shuffle_receive_process, worker->num_targetProcess, MPI_INT, 
		manager_rank, MPI_COMM_WORLD);
}

void SumTotalNumOfWordsInShuffle_Worker(Worker *worker) {
	MPI_Reduce(&worker->num_shuffle_local, &worker->num_shuffle_total, 1, MPI_INT, 
		MPI_SUM, manager_rank, MPI_COMM_WORLD);
}

void ShuffleRecv_Worker(Worker *worker, int proc_rank, MPI_Datatype wordcount_t) {
	while (true) {
		if (isTarget(worker->shuffle_receive_process, worker->num_targetProcess, proc_rank)) {
			MPI_Status status_shuffle;
			word_count_pair temp[1];
			memset(temp, 0, sizeof(word_count_pair));

			MPI_Recv(temp, 1, wordcount_t, MPI_ANY_SOURCE, MPI_ANY_TAG, 
				MPI_COMM_WORLD, &status_shuffle);
			MPI_Send(MPI_BOTTOM, 0, MPI_INT, manager_rank, tag_term_sig, MPI_COMM_WORLD);
			if (status_shuffle.MPI_SOURCE == manager_rank) {
				break;
			}
			worker->partialResultCounts = combineHistogram(worker->locallyRecvdPartialResult, 
				worker->partialResultCounts, temp, 1);
		}
		else
			break;
	}
}

void ShuffleWaitall_Worker(Worker *worker) {
	MPI_Waitall(worker->num_shuffle_local, worker->request, MPI_STATUS_IGNORE);
}

void GatherResultInWorkerComm_Worker(Worker *worker, MPI_Comm worker_comm, MPI_Datatype wordcount_t) {
	/* Gather locally distributed histograms to rank 0 processor of the worker_comm. */
	MPI_Reduce(&worker->partialResultCounts, &worker->result_totalCounts, 1, MPI_INT, 
		MPI_SUM, 0, worker_comm);

	MPI_Allgather(&worker->partialResultCounts, 1, MPI_INT, worker->partialResultCounts_array, 1, MPI_INT, worker_comm);

	int displacements[NUM_PROCESS - 1] = { 0 };
	for (int i = 1; i < NUM_PROCESS - 1; i++) {
		displacements[i] = worker->partialResultCounts_array[i - 1] + displacements[i - 1];
	}

	MPI_Gatherv(worker->locallyRecvdPartialResult, worker->partialResultCounts, wordcount_t,
		worker->result, worker->partialResultCounts_array, displacements, wordcount_t, 0, worker_comm);
}

/* Send result to manager. */
void SendResultToManager_Worker(Worker *worker, MPI_Comm worker_comm, MPI_Datatype wordcount_t) {
	int myrankinworker;
	MPI_Comm_rank(worker_comm, &myrankinworker);
	if (myrankinworker == 0) {
		MPI_Send(&worker->result_totalCounts, 1, MPI_INT, manager_rank, 
			tag_gather_result, MPI_COMM_WORLD);
		MPI_Send(worker->result, worker->result_totalCounts, wordcount_t, 
			manager_rank, tag_gather_result, MPI_COMM_WORLD);
	}
}

#endif WORKER_H
