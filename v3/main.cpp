/**********************************************************************
* This code uses MPI library to count word frequencies distributed in *
* several files. Manager-worker pattern is used for self-scheduling.  *
* <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> *
* 其实没有必要采用Manager-Worker的模式，这里为了练手所以采用了这种做法.   *
* <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> *
* Author: Dawei Wang												  *
* Date: 2017-07-03.												      *
***********************************************************************/

#include "common.h"
#include "manager.h"
#include "worker.h"

int main(int argc, char *argv[]) {
	int numprocess, proc_rank;
	Manager manager;
	// 初始化Manager
	Init_Manager(&manager);

	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocess);
	MPI_Comm_rank(MPI_COMM_WORLD, &proc_rank);
	printf("MPI processor %d has started ...\n", proc_rank);

	MPI_Datatype wordcount_t = createWordCountPairType();

	/* Create a seperate workers communicator from MPI_COMM_WORLD. */
	// 为workers单独创建一个Communicator，因为在Gather result的时候，Manager
	// 是不能参与的，只能在worker communicator中gather好了再发送给Manager。
	// 再次重申，这个问题实际上是不需要采用Manager-worker模式的，这样做反而使得
	// 问题稍微复杂化了，之所以采用是为了练手
	MPI_Comm worker_comm;
	createWokerComm(MPI_COMM_WORLD, &worker_comm);

	/******************** Manager Part **********************/

	if (proc_rank == manager_rank) {

		const char fileName[] = "data\\filelist.txt";

		// 获取任务文件路径，以及任务个数
		GetTasks_Manager(&manager, fileName);

		// Broadcast总任务个数，因为各个进程需要知道这一信息以判断自己是否能分到任务
		BcastTaskCounts_Manager(&manager);

		// 一次性分发第一批任务到各个进程
		DispatchTasks1_Manager(&manager, wordcount_t);

		// 响应worker的work request请求，如果有未完成任务则发给相应进程，
		// 否则发出信号改变这一进程的状态，表示其正处于idle，准备就绪进入下一阶段
		RespondToWorkReq_Manager(&manager, wordcount_t);

		// 接收所有worker发来的ShuffleIssend完毕的信号，注意这里只表示Issend已执行，
		// 但并不表示已返回，即并不表示已被Recv
		CheckAllCompleteShuffle_Manager(&manager);

		// 将所有参与Shuffle需要Recv的进程的rank列表广播出去
		BcastShuffleProcs_Manager(&manager);

		// 计算所有进程中参与Shuffle的单词的个数总和，用来指导Manager应当在什么时候
		// 发送termination signal，防止其提前发送以干扰某些进程接收shuffle words
		SumTotalNumOfWordsInShuffle_Manager(&manager);

		// Manager每收到一个Recv完毕的信号，就将其记录下，当收到的信号个数等于
		// 上一步求得的总和时，就表示所有Issend出去的单词都被接收完毕，可以发送
		// termination signal了
		RecordingShuffleWordsRecvd_Manager(&manager);

		// 发送termination signal以终止所有worker进程的Recv循环
		SendTermSigToShuffleRecv_Manager(&manager, wordcount_t);

		// 回收最终计算结果
		GetFinalResult_Manager(&manager, wordcount_t);
	}

	/******************** Worker Part **********************/

	else {
		Worker worker;
		// 初始化Worker
		Init_Worker(&worker);

		// 获取总任务个数
		BcastTaskCounts_Worker(&worker);

		// 如果worker的proc_rank比总的任务个数要小，则其可以分配到任务
		if (proc_rank <= worker.task_counts) {
			while (true) {
				// 接收并处理任务，处理完成后请求下一任务
				RecvAndProcessTasks_Worker(&worker, wordcount_t);

				// 向Manager发出work request请求
				int Got = RequireWork_Worker();
				if (!Got)
					break;
			}

			// 进入Shuffle阶段，将每个word发送到指定的进程
			ShuffleIssend_Worker(&worker, proc_rank, wordcount_t);

			// 将Shuffle阶段需要接收单词的所有进程的rank的列表广播出去
			BcastShuffleProcs_Worker(&worker);

			// 计算所有进程中参与Shuffle的单词的个数总和，用来指导Manager应当在什么时候
			// 发送termination signal，防止其提前发送以干扰某些进程接收shuffle words
			SumTotalNumOfWordsInShuffle_Worker(&worker);

			// 创建循环接收Shuffle words，如果收到的信息来自Manager，则退出循环停止接收
			ShuffleRecv_Worker(&worker, proc_rank, wordcount_t);

			// Wait所有的异步发送Issend
			ShuffleWaitall_Worker(&worker);

			// Gather最终的result到worker_comm中rank 0的进程
			GatherResultInWorkerComm_Worker(&worker, worker_comm, wordcount_t);

			// 将最终结果发送给Manager
			SendResultToManager_Worker(&worker, worker_comm, wordcount_t);
		}

		// 如果worker的proc_rank比总的任务个数要大，则其不可能分配到任务，
		// 因此需要将其关闭退出工作空间，也即Finalize()
		else {
			MPI_Finalize();
		}
	}

	MPI_Type_free(&wordcount_t);
	MPI_Finalize();

	return 0;
}