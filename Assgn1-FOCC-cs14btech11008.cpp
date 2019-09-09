#include <iostream>
#include <thread>
#include <fstream>
#include <shared_mutex>
#include <mutex>
#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include <sstream>
#include <cmath>
#include <vector>
#include <stdio.h>
#include <algorithm>
#include <set>
#include <time.h>
#include <chrono>
#include <random>

/*

SIZE: size of the data items array
lambda: exponential delay parameter
readSet[i]: stores the data-items read by transaction i until now.
writeSet[i]: stores the data-items written by transaction i until now.
liveTrans: stores the ids of transactions are running

*/

const int SIZE = 10;
const int lambda = 3;
int n, m;
std::vector<std::set<int> > readSet;
std::vector<std::set<int> > writeSet;
std::vector<int> liveTrans;

/*

We record the total time taken to complete transactions that are eventually committed 
and the number of committed transactions to calculate average transaction time.
NOTE: We don't consider aborted transactions to compute the avg trans time.

*/

long long total_time_taken = 0;
int num_commits = 0;

/*

opNode stores details of a single operation -
op_type can be 'b' - begin, 'd' - delay, 'r' - read, 'w' - write or 'c' - commit
index denotes data item to read or write to.

*/

struct opNode {
	char op_type;
	int index;
};

/*
The scheduler coordinates access to the common data array by different transactions.
private variables:
data: the data items array (vector)
rw_mutex: mutex lock to execute val-write phase as a critical section. 
mtx: mutex lock to add/remove a transaction from liveTrans array.

public methods:
constructor: 
	- initilise data items array with random elements between 1 to 10.
addToLive (int t): 
	- add transaction t to the liveTrans array.
removeFromLive (int t, bool success, long time_taken): 
	- remove transaction t from the liveTrans array.
	- if it has been committed then increment num_commits and add transaction time to the total_time_taken.
getData (int tid, int currTrans, int index):
	- rw_mtx is locked so that no read from the database is concurrent 
	  with a val-write phase of some other transaction.
valWrite (int currTrans, std::vector<int> &local_data)
	- validate against all live transactions t
		- if t is not equal currTrans
			- if readSet[t] intersection with writeSet[currTrans] is null, validated against t
		  	- else currTrans has to be aborted because of a possible conflict with t, return t
	- if validated against all, then copy local data items to the database and return -1
*/

class scheduler {
private:
	std::vector<int> data;
	std::mutex rw_mtx; std::mutex mtx;
public:
	scheduler () {
		// initial data items array with a random number between 1 to 10
		data.resize (SIZE);
		for (int i=0; i<SIZE; i++) {
			data[i] = rand() % 10 + 1;
		}
		printf("Initial data array: ");
		for (auto x : data) {
			printf("%d ", x);
		}
		printf("\n");
	}

	void addToLive (int t) {
		mtx.lock ();
		liveTrans.push_back (t);
		mtx.unlock();
	}

	void removeFromLive (int t, bool success, long time_taken) {
		mtx.lock ();
		liveTrans.erase (std::remove(liveTrans.begin(), liveTrans.end(), t), liveTrans.end());
		if (success) {
			num_commits += 1;
			total_time_taken += time_taken;
		}
		mtx.unlock();
	}

	void getData (int tid, int currTrans, int index) {
		rw_mtx.lock();
		readSet[currTrans].insert (index);
		printf("Thread %d: Reading data[%d] = %d for transaction #%d\n", tid, index, data[index], currTrans);
		rw_mtx.unlock();
	}

	int valWrite (int currTrans, std::vector<int> &local_data) {
		rw_mtx.lock();
		std::vector<int> intersect;
		for (auto t : liveTrans) {
			if (t == currTrans) {
				continue;
			}
			set_intersection (readSet[t].begin(), readSet[t].end(), 
				writeSet[currTrans].begin(), writeSet[currTrans].end(),
				std::back_inserter (intersect));
			if (!intersect.empty()) {
				rw_mtx.unlock();
				return t;
			}
		}
		if (intersect.empty()) {
			for (int i=0; i<SIZE; i++) {
				if (local_data[i] != -1)
					data[i] = local_data[i];
			}
			rw_mtx.unlock();
			return -1;
		}
	}

};

/*
Method to generate a delay time that is exponentially distributed over parameter lambda.
*/

int delay (int x) {
	int sd = std::chrono::system_clock::now().time_since_epoch().count();
	std::default_random_engine generator(sd);
	std::exponential_distribution <double> distribution(1.0/x);
	return (int) distribution(generator);
}

/*
transArr: array of all transactions. Each trans is a vector of opNode
S: instance of scheduler
*/

std::vector<std::vector<opNode> > transArr;
scheduler S;

/*
testTrans method for thread #i
local_data: private version of data items
*/

void testTrans (int i) {
	
	std::vector<int> local_data (SIZE, -1);
	int currTrans; int temp;
	auto start = std::chrono::steady_clock::now();
	auto end = std::chrono::steady_clock::now();
	long time_taken;
	int failure; bool success;
	for (int transIter = 0; transIter < ceil((m * 1.0)/n); transIter++) {

		currTrans = transIter * n + i;
		if (currTrans > m-1) 
			continue;
		std::fill(local_data.begin(), local_data.end(), -1);	// reset local data items

		for (auto x : transArr[currTrans]) {

			switch (x.op_type) {

				case 'b':
					start = std::chrono::steady_clock::now();
					S.addToLive (currTrans);
					printf("Thread %d beginning transaction %d\n", i, currTrans);
					break;
				case 'r':
					S.getData (i, currTrans, x.index);
					break;
				case 'w':
					temp = rand() % 10 + 1;
					local_data[x.index] = temp;
					printf("Thread %d: Writing data[%d] = %d for transaction #%d\n", i, x.index, local_data[x.index], currTrans);
					writeSet[currTrans].insert (x.index);
					break;
				case 'd':
					// sleep (delay(lambda));
					usleep((rand() % 10 + 1) * 1000);
					break;
				case 'c':
					failure = S.valWrite (currTrans, local_data);
					end = std::chrono::steady_clock::now();
					time_taken = std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count();
					if (failure == -1) {
						printf("Thread %d: Committed transaction #%d successfully\n", i, currTrans);
					} else {
						printf("Thread %d: Aborted transaction #%d due to conflict with transaction #%d\n", i, currTrans, failure);
					}
					success = (failure == -1);
					S.removeFromLive (currTrans, success, time_taken);
					break;
			}
		}	
	}
}

int main () {
	srand (time(NULL));

	freopen ("inp-params.txt", "r", stdin);
	freopen("output-focc.txt", "w", stdout);

	std::cin >> n >> m; // num_threads num_transactions
	transArr.resize(m);
	readSet.resize (m, std::set<int> ());
	writeSet.resize (m, std::set<int> ());
	
	// read input-params file and populate transArr
	std::string temp;
	getline (std::cin, temp); // dummy for catching the new line char of line 1
	for (int i=0; i<m; i++) {
		getline(std::cin, temp);
		std::istringstream iss(temp);
		std::string op;
		while (getline(iss, op, ' ') ) {
			opNode a;
			if (op[0] == 'r' || op[0] == 'w') {
				a.op_type = op[0];
				a.index = op[1] - '0';
			} else {
				a.op_type = op[0];
			}
			transArr[i].push_back(a);
		}
	}

	std::thread *workers;
	workers = new std::thread [n];
	for (int i=0; i<n; i++) {
		workers[i] = std::thread (testTrans, i);
	}
	for (int i=0; i<n; i++) {
		workers[i].join();
	}
	printf("Total time taken to commit %d transactions is %lld milliseconds\n", num_commits, total_time_taken);
	printf("Average time for a transaction to commit is %f milliseconds\n", total_time_taken / (num_commits * 1.0));
	return 0;
}

