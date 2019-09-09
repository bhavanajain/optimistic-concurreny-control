#include <bits/stdc++.h>
#include <iostream>
#include <sstream>
#include <string>
#include <algorithm>
#include <iterator>
#include <vector>
#include <set>
#include <thread>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>

using namespace std;

struct op {
    char op_type;
    int index;
};

struct trans_tuple {
    int index;
    int value;
    long int wts;
    long int rts;
};

struct data_tuple {
    int value;
    long int wts;
    long int rts;
};

int n, m;   // n -> number of threads, m -> number of transactions
vector<vector<op> > transactions (n);
int num_commits = 0;

bool compareByIndex (const trans_tuple &a, const trans_tuple &b){
    return a.index < b.index;
}

class TicToc{     //the TicToc scheduler class
public:   
    int size;
    vector<data_tuple> dataitems;                   //the data items
    pthread_rwlock_t rwlock;    //lock for critical section
    vector<bool> locked;
    
    TicToc() {                                       //constructor
        size = 10;
        dataitems.resize (size);
        locked.resize (size, false);
        pthread_rwlock_init(&rwlock, NULL);          //intializing the lock

        printf("Initial dataitems value: ");        //initializing the data items
        for (int i=0; i<size; i++) {
            dataitems[i].value = rand() % 10 + 1;
            dataitems[i].wts = dataitems[i].rts = 0;
            printf("%d ", dataitems[i].value);
        }
        printf("\n");
    }

    // reads the data under lock
    void read_data(int trans_id, struct trans_tuple *one, int thread_id){
        
        pthread_rwlock_rdlock(&rwlock);     //to ensure that others don't read while a transaction is in its critical section
        one->value = dataitems[one->index].value;     //reads the data item into readset
        one->wts = dataitems[one->index].wts;
        one->rts = dataitems[one->index].rts;
        printf("%ld: Reading %d for dataitem:%d for transaction %d invoked by thread %d\n",clock(), dataitems[one->index].value, one->index, trans_id, thread_id);
        pthread_rwlock_unlock(&rwlock);     //unlock        
        return ;
    }
    
    // validation and committing the changes
    void valwrite(int trans_id, int thread_id, vector<trans_tuple> readset, vector<trans_tuple> writeset, vector<trans_tuple> diff){
        bool success = true;
        long int commit_ts = 0;
        set<int> ws;

        pthread_rwlock_wrlock(&rwlock);   //to ensure that critical section is atomic
        for (auto x: writeset){
            // pthread_rwlock_wrlock(&rwlocks[x.dataitem]);
            ws.insert(x.index);
            locked[x.index] = true;
        }

        // computing the timestamp lazily from the metadata
        commit_ts = 0;
        for (auto x : writeset){
            commit_ts = max(commit_ts, dataitems[x.index].rts + 1);
        }
        for (auto x : diff){
            commit_ts = max(commit_ts, x.wts);
        }

        // validating it's reads
        for (auto x : readset) {
            if (x.rts < commit_ts) {
                if ((x.wts != dataitems[x.index].wts) || 
                    (dataitems[x.index].rts <= commit_ts 
                        && locked[x.index] 
                            && ws.find(x.index) == ws.end())) {
                    success = false;
                    break;
                } else {
                    dataitems[x.index].rts = max (commit_ts, dataitems[x.index].rts);
                }
            }
        }

        if (success){
            for (auto x : writeset){      //make the changes gloabal
                dataitems[x.index].value = x.value;  //commit
                dataitems[x.index].wts = dataitems[x.index].rts = commit_ts;
            }
            printf("%ld: Thread %d committed transaction #%d\n",clock(), thread_id, trans_id);
            num_commits += 1;           
        }
        else    
            printf("%ld: Thread %d aborted transaction #%d\n",clock(), thread_id, trans_id);

        for (auto x: writeset){
            // pthread_rwlock_unlock(&rwlocks[x.dataitem]);
            locked[x.index] = false;
        }
        pthread_rwlock_unlock(&rwlock);   //unlock - critical section ends
        
        return;
    }

    void testTrans(int thread_id){
       
        struct trans_tuple one;
        vector<trans_tuple> readset;             //dataitems transactions reads
        vector<trans_tuple> writeset;           //dataitems transactions writes
        vector<trans_tuple> diff;
        
        int trans_id = thread_id;
        while(trans_id < m) {
        
            readset.clear();            //clear the local variables for next transaction
            writeset.clear();
            diff.clear();

            for (auto x : transactions[trans_id]) {  //for operations in a transaction
                switch (x.op_type) {
                    case 'b': {     //begin
                        printf("%ld: Thread %d beginning transaction %d\n",clock(), thread_id, trans_id);
                        break;
                    }
                    case 'r': {     //read
                        one.index = x.index;
                        readset.push_back(one);
                        read_data (trans_id, &readset[readset.size()-1], thread_id);
                        break;
                    }    
                    case 'w': {     //write
                        one.index = x.index;
                        one.value = rand() % 10 + 1;
                        one.wts = dataitems[one.index].rts + 1;
                        writeset.push_back(one); //adds to writeset
                        printf("%ld: Writing %d for dataitem:%d for transaction %d invoked by thread %d\n", clock(), one.value, one.index, trans_id, thread_id);
                        break;
                    }
                    case 'd': {     //delay
                        usleep((rand() % 10 + 1) * 1000);
                        break;
                    }
                    case 'c': {     //try-commit
                        sort(writeset.begin(), writeset.end(),compareByIndex);
                        sort(readset.begin(), readset.end(), compareByIndex);
                        // TODO
                        set_difference(readset.begin(), readset.end(), 
                            writeset.begin(), writeset.end(), 
                            back_inserter(diff), compareByIndex);

                        valwrite (trans_id, thread_id, readset, writeset, diff);
                        break;
                    }
                }  //end of switch 
            } //end of for loop
            trans_id += n;  
        } //end of while loop
        return;
    }

}; //end of class TicToc


int main(){
    
    fstream file;
    string filename,line, str1;
    filename = "inp-params.txt";            //name of input file
    file.open(filename.c_str());
  
    file >> n >> m;       //reading from file
    getline(file, line);

    transactions.resize(m);

    struct op temp;
    temp.index = 0;
    int trans_counter = 0;
    while (trans_counter < m){       //reading all transactions from file
        getline(file, line);
        istringstream iss(line);            //breaking the line at space
        vector<string> tokens{istream_iterator<string>{iss},
                      istream_iterator<string>{}};
        for(auto str1: tokens){             //preparing the transaction format 
            if(str1[0] == 'r' || str1[0] == 'w'){
                temp.op_type = str1[0];
                temp.index = str1[1]-'0';
            }
            else
                temp.op_type = str1[0];
            transactions[trans_counter].push_back(temp);
        }
        trans_counter++;   
    }
    
    /*cout<<"\n"<<trans_counter<<endl;
    for(auto i: transactions){
        for (auto j: i){
            cout<<j.operation<<j.dataitem<<", ";  
        }
        cout<<endl;
    }*/

    TicToc *sched = new TicToc();  //constructor

    thread thd[n];
    for (int i = 0; i < n; i++)   //allocating threads
        thd[i] = thread( &TicToc::testTrans, sched, i);

    for (int i = 0; i < n; i++)       //waiting for join
        thd[i].join();

    printf("# committed transactions : %d\n", num_commits);
    return 0; 
}