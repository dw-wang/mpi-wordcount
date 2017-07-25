/*********************************************************************** 
 * This code uses MPI library to count word frequencies distributed in *
 * several files. Manager-worker pattern is used for self-scheduling.  *
 * Author: Dawei Wang												   *
 * Date: 2017-07-03.												   *
 ***********************************************************************/

#include "common.h"

int main(int argc, char *argv[]) {
	int proc_rank, numprocess, manager = 0, 
		tag_rawdata = 1,
		tag_requirework = 2, 
		tag_dispatchinfo = 3, 
		tag_shuffle = 4, 
		/* Note that we need to differentiate these two status tag, 
		   or there might be a deadlock caused by processor race!!! */
		// 注意这里出过错！！！
		tag_worker_status_change_1 = 5,
		tag_worker_status_change_2 = 6,
		tag_final = 7,
		tag_get_shuffle_procs = 8,
		tag_tell_manager = 9,
		tag_term_sig = 10,
		tag_gather_result = 11;

	word_count_pair wcp_s[MAX_WORD_COUNT], wcp_r[MAX_WORD_COUNT], 
		localcounted_array[MAX_WORD_COUNT];
	word_count_pair locallyRecvdPartialResult[MAX_WORD_COUNT];
	word_count_pair result[TOTAL_MAX_WORD_COUNT];

	memset(wcp_s, 0, MAX_WORD_COUNT*sizeof(word_count_pair));
	memset(wcp_r, 0, MAX_WORD_COUNT*sizeof(word_count_pair));
	memset(locallyRecvdPartialResult, 0, MAX_WORD_COUNT*sizeof(word_count_pair));
	memset(result, 0, MAX_WORD_COUNT*sizeof(word_count_pair));
	memset(localcounted_array, 0, MAX_WORD_COUNT*sizeof(word_count_pair));

	int task_counts;
	int num_localcountedwords = 0;
	int partialResultCounts = 0;
	int partialResultCounts_array[NUM_PROCESS - 1] = { 0 };
	int result_totalCounts = 0;
	int WorkerStatus[NUM_PROCESS - 1] = { WORKER_RECV_TASK_STATUS };

	int num_targetProcess = 0;
	int shuffle_receive_process[NUM_PROCESS - 1];
	memset(shuffle_receive_process, 0, (NUM_PROCESS - 1) * sizeof(int));

	int num_shuffle_local = 0;
	int num_shuffle_total = 0;

	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocess);
	MPI_Comm_rank(MPI_COMM_WORLD, &proc_rank);
	printf("MPI processor %d has started ...\n", proc_rank);

	/* Create a seperate workers communicator from MPI_COMM_WORLD. */
	MPI_Comm worker_comm;
	createWokerComm(MPI_COMM_WORLD, &worker_comm);

	/* Define word-count pair derived data type */
	MPI_Datatype wordcount_t = createWordCountPairType();

	if (proc_rank == manager) {

		/* Prepration step: read file list */
		char fpaths_array[LINE_MAX_LEN * MAX_TASKS];
		char *fpaths_p[MAX_TASKS];

		char path[1024];
		const char fileName[] = "data\\filelist.txt";
		genFullFilePath(path, fileName);

		task_counts = getDataPaths(path, fpaths_array, fpaths_p);

		MPI_Bcast(&task_counts, 1, MPI_INT, manager, MPI_COMM_WORLD);

		/************************ Manager Part **************************/

		/* Read each file in the list and distribute file tasks */
		int task_sent = 0;
		for (int i = 0; i < MIN(NUM_PROCESS - 1, task_counts); i++) {
			int target_rank = i % (NUM_PROCESS - 1) + 1;

			char task_fullpath[1024];
			genFullFilePath(task_fullpath, fpaths_p[i]);
			
			int w_counts = 0;
			readWordsToArray(wcp_s, &w_counts, task_fullpath);

			MPI_Send(wcp_s, w_counts, wordcount_t, target_rank, tag_rawdata, MPI_COMM_WORLD);
			task_sent++;

		}
		printf("First batch of task sending completed successfully!\n\n");

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

				if (checkAllWorkerStatus(WorkerStatus, WORKER_READY_TO_SHUFFLE_STATUS, MIN(NUM_PROCESS - 1, task_counts))) {
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
		// 这里肯定有错误，因为下面Bcast后打印出来的不对！可就是找不出来在哪！
		// 更新：错误找出来了，原来确实是setTargetArray有错，里面的一个大括号丢了！
		for (int i = 0; i < MIN(NUM_PROCESS - 1, task_counts); i++) {
			printf("To Receive processor ids involved in shuffle stage ......\n");

			int recvd_array[NUM_PROCESS - 1] = { 0 };
			MPI_Status status;
			MPI_Recv(recvd_array, NUM_PROCESS - 1, MPI_INT, MPI_ANY_SOURCE, tag_get_shuffle_procs,
				MPI_COMM_WORLD, &status);
			int from_proc = status.MPI_SOURCE;
			int num_recvd = 0;
			MPI_Get_count(&status, MPI_INT, &num_recvd);
			printf("num recvd from processor %d is: %d\n", i, num_recvd);
			num_targetProcess = setTargetArray(shuffle_receive_process, num_targetProcess, recvd_array, num_recvd);

			printf("Received processor ids involved in shuffle stage from processor %d!\n", from_proc);
			printf("Recvd processor array are:\n");
			for (int j = 0; j < num_recvd; j++) {
				printf("%d\t", recvd_array[j]);
				if (j == num_recvd - 1)
					printf("\n\n");
			}
		}
		
		printf("Broadcasting shuffle processor ranks array to all workers!\n");
		MPI_Bcast(&num_targetProcess, 1, MPI_INT, manager, MPI_COMM_WORLD);
		MPI_Bcast(shuffle_receive_process, num_targetProcess, MPI_INT, manager, MPI_COMM_WORLD);
		printf("Broadcasting completed!\n\n");

		MPI_Reduce(&num_shuffle_local, &num_shuffle_total, 1, MPI_INT, MPI_SUM, manager, MPI_COMM_WORLD);
		printf("Number of words involved in shuffle: %d\n\n", num_shuffle_total);

		for (int i = 0; i < num_shuffle_total; i++) {
			MPI_Recv(MPI_BOTTOM, 0, MPI_INT, MPI_ANY_SOURCE, tag_term_sig, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}

		// 这个地方原先不对，因为对应到worker的代码，worker要先判断自己是不是target，是才会接收，
		// 所以Manager要Send的数目是target process的数目，而不是MIN(NUM_PROCESS - 1, task_counts).
		for (int i = 1; i <= num_targetProcess; i++) {
			printf("Begin sending final signal!\n");
			/* Although the corresponding Recv can listen on MPI_ANY_TAG and MPI_ANY_SOURCE,
			   the DataType in Send and Recv must match, so here we need to use wordcount_t. */
			MPI_Ssend(MPI_BOTTOM, 0, wordcount_t, i, tag_final, MPI_COMM_WORLD);
			printf("Manager final signal sent!\n");
		}

		int ready = checkAllWorkerStatus(WorkerStatus, WORKER_READY_TO_GATHER_STATUS, MIN(NUM_PROCESS - 1, task_counts));
		if (ready) {
			printf("Haha, I entered the final stage and begin to receive the result!\n");
			MPI_Recv(&result_totalCounts, 1, MPI_INT, MPI_ANY_SOURCE, tag_gather_result, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			MPI_Recv(result, result_totalCounts, wordcount_t, MPI_ANY_SOURCE, tag_gather_result, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}
		printf("\nTotal counts: %d\n\n", result_totalCounts);
		printWCPtoFile(stdout, result, result_totalCounts);
	}

	/****************************** Worker Part *******************************/

	else {
		MPI_Bcast(&task_counts, 1, MPI_INT, manager, MPI_COMM_WORLD);

		if (proc_rank <= task_counts) {
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
				/*********************************************************************/

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
					/*********************************************************************/

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
					FILE *fp_Issend = fopen(debug_Issend, "a+");
					fprintf(fp_Issend, "Begin Issending word %s to processor %d.\n", localcounted_array[i].word, target_proc);
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

			// 其实可以考虑采用MPI_Allgatherv将targ_proc收集起来，但是这样的话收集起来的target_array
			// 会有很多duplicate，最糟糕的情况是(NUM_PROCESS - 1)^2阶，如果收集之前不去重的话情况更糟，
			// 而且收集之后还是得去重，因为下面要用到总数，而且下面用isTarget()查询的时候会很慢，具体这里还有待商榷。
			// 注意这里出过错！！！
			MPI_Send(targ_proc, num_tp, MPI_INT, manager, tag_get_shuffle_procs, MPI_COMM_WORLD);

			MPI_Bcast(&num_targetProcess, 1, MPI_INT, manager, MPI_COMM_WORLD);
			MPI_Bcast(shuffle_receive_process, num_targetProcess, MPI_INT, manager, MPI_COMM_WORLD);
			/************************ Debug section *****************************/
			char debug_targranks[1024];
			char fileName8[] = "debug_targranks";
			genFullFilePath(debug_targranks, fileName8);
			FILE *fp_targranks = fopen(debug_targranks, "a+");
			fprintf(fp_targranks, "Target processor ranks are: \t");
			for (int i = 0; i < num_targetProcess; i++) {
				fprintf(fp_targranks, "%d\t", shuffle_receive_process[i]);
				if (i == num_targetProcess - 1) {
					fprintf(fp_targranks, "\n");
				}
			}
			fclose(fp_targranks);
			/**********************************************************************/

			num_shuffle_local = req_index;
			MPI_Reduce(&num_shuffle_local, &num_shuffle_total, 1, MPI_INT, MPI_SUM, manager, MPI_COMM_WORLD);

			while (true) {
				if (isTarget(shuffle_receive_process, num_targetProcess, proc_rank)) {
					MPI_Status status_shuffle;
					word_count_pair temp[1];
					memset(temp, 0, sizeof(word_count_pair));

					MPI_Recv(temp, 1, wordcount_t, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status_shuffle);
					/************************ Debug section *****************************/
					char debug_haha[1024];
					char fileNameeee[] = "debug_haha";
					genFullIndexedFilePath(debug_haha, fileNameeee, proc_rank);
					FILE *fp_debughaha = fopen(debug_haha, "a+");
					fprintf(fp_debughaha, "Recvd\n");
					fclose(fp_debughaha);
					/**********************************************************************/
					MPI_Send(MPI_BOTTOM, 0, MPI_INT, manager, tag_term_sig, MPI_COMM_WORLD);
					if (status_shuffle.MPI_SOURCE == manager) {
						break;
					}
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

			/* Gather locally distributed histograms to rank 0 processor of the worker_comm. */
			MPI_Reduce(&partialResultCounts, &result_totalCounts, 1, MPI_INT, MPI_SUM, 0, worker_comm);

			MPI_Allgather(&partialResultCounts, 1, MPI_INT, partialResultCounts_array, 1, MPI_INT, worker_comm);

			int displacements[NUM_PROCESS - 1] = { 0 };
			for (int i = 1; i < numprocess - 1; i++) {
				displacements[i] = partialResultCounts_array[i - 1] + displacements[i - 1];
			}

			MPI_Gatherv(locallyRecvdPartialResult, partialResultCounts, wordcount_t,
				result, partialResultCounts_array, displacements, wordcount_t, 0, worker_comm);

			/* Send result to manager. */
			int myrankinworker;
			MPI_Comm_rank(worker_comm, &myrankinworker);
			if (myrankinworker == 0) {
				MPI_Send(&result_totalCounts, 1, MPI_INT, manager, tag_gather_result, MPI_COMM_WORLD);
				MPI_Send(result, result_totalCounts, wordcount_t, manager, tag_gather_result, MPI_COMM_WORLD);
			}
		}
		else {
			MPI_Finalize();
		}
	}

	MPI_Type_free(&wordcount_t);
	MPI_Finalize();

	return 0;
}
