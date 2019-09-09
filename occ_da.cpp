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

struct operations {
    char operation;
    int dataitem;
};

struct change_data {    //structure to maintain local data
    int ind;
    int val;
};

struct record_trans {      //structure to maintain committed transactions
    int transaction_id;
    long int sot;  //end of valwrite phase
};

struct  Ti{
    int transid;
    long int sot;
    vector<int> readset;
    vector<int> tr;
    vector<int> writeset;
};

struct data_tuple{
    int data;
    long int rts;
    long int wts;
};

int num_threads, num_trans; 
vector<vector<operations> > transactions(num_trans);   //history
int committed = 0;

class OCC_DA{     //the OCC_DA scheduler class    

public:   
    
    int datasize;
    vector<data_tuple> dataitems;                   //the data items
    pthread_rwlock_t rwlock;                //lock for critical section
    // vector<pthread_rwlock_t> rwlocks;
    vector<Ti *> livetrans;
    mutex some_lock;
    
    OCC_DA(){                                 //constructor
        datasize = 10;
        dataitems.resize (datasize);
        pthread_rwlock_init(&rwlock,NULL);   //intializing the lock

        printf("Initial dataitems value: ");    //initializing the data items
        for (int i=0; i<datasize; i++) {
            dataitems[i].data = rand() % 10 + 1;
            dataitems[i].rts = 0;
            dataitems[i].wts = 0;
            printf("%d ", dataitems[i].data);
            // pthread_rwlock_t temp;
            // rwlocks.push_back(temp);
            // pthread_rwlock_init(&rwlocks[rwlocks.size()-1],NULL); 
        }
        printf("\n");
    }
 
    void addme_tolive(struct Ti *thisone){
        pthread_rwlock_wrlock(&rwlock);
        livetrans.push_back(thisone);
        pthread_rwlock_unlock(&rwlock);
        return;
    }

    //reads the data under lock
    void read_data(int trans_id, int dataitem_id, struct Ti *tran_i, int thread_id){
        pthread_rwlock_rdlock(&rwlock);     //to ensure that others don't read while a transaction is in its critical section
        tran_i->readset.push_back(dataitem_id);     //reads the data item into readset
        tran_i->tr.push_back(dataitems[dataitem_id].wts);
        // tran_i->rts.push_back(dataitems[dataitem_id].rts);
        printf("%ld: Reading %d for dataitem:%d for transaction %d invoked by thread %d\n",clock(), dataitems[dataitem_id].data, dataitem_id, trans_id,thread_id);
        pthread_rwlock_unlock(&rwlock);     //unlock
        return;
    }

    //validation and committing the changes
    void valwrite(int trans_id, int thread_id, vector<change_data> localdata, struct Ti present){
        int status = -1;
        pthread_rwlock_wrlock(&rwlock);             //to ensure that critical section is atomic
        vector<int> intersection;
        vector<int> BTlist;

        // for (auto Tj : livetrans){
        //     printf("%d:%d...\n",clock(), Tj->transid);
        // }

        if (present.sot != INT_MAX){
            // printf("my timestamp not inf:%d\n",present.transid);
            for (int ind=0; ind<present.readset.size();ind++){
                if(present.tr[ind] > present.sot){
                    printf("transaction #%d needs to be restarted\n", trans_id);
                    status = 1;
                    break;
                }   
            }
            if (status == -1){
                for (int ind=0; ind<present.writeset.size();ind++){
                    if(present.sot < dataitems[present.writeset[ind]].rts || present.sot < dataitems[present.writeset[ind]].wts){
                        printf("transaction #%d needs to be restarted\n", trans_id);
                        status = 1;
                        break;
                    }   
                }
            }
        }

        if (status == -1){
            //write(Tv) - read(Tj) conflict
            for (auto Tj : livetrans){
                if (Tj->transid != present.transid && Tj->sot <= present.sot ){
                    // printf("tj: %d\n",Tj->transid );
                    for(auto Dp : present.writeset){
                        if(find(Tj->readset.begin(), Tj->readset.end(), Dp) != Tj->readset.end()){
                            BTlist.push_back(Tj->transid);
                            //printf("inserted%d\n",Tj->transid );
                            //status = 1;
                            // break;
                        }
                    }
                }
            }

            for (auto x : BTlist){
                printf("write(Tv)-read(Tj) conflict: %d-->%d. current transaction#%d needs to be backwards adjusted\n",x,trans_id,trans_id );
            }

            for (auto Tj : livetrans){
                //write(Tj) - read(Tv) conflict
                if (Tj->transid != present.transid && (Tj->sot < present.sot || find(BTlist.begin(), BTlist.end(),Tj->transid) != BTlist.end())){
                    for (auto Dp : present.readset){
                        if (find(Tj->writeset.begin(),Tj->writeset.end(),Dp) != Tj->writeset.end()){
                            //conflict_resolution(present,Tj);
                            status = 1;
                            printf("ABORT:read(Tv)-write(Tj) conflict: %d-->%d. transaction#%d needs to be forwards adjusted\n",trans_id,Tj->transid,Tj->transid);
                            break;
                        }
                    }
                    if (status == 1)
                        break;
                    //write(Tj) - write(Tv) conflict
                    for (auto Dp : present.writeset){
                        if (find(Tj->writeset.begin(),Tj->writeset.end(),Dp) != Tj->writeset.end()){
                            //conflict_resolution(present,Tj);
                            status = 1;
                            printf("ABORT:write(Tv)-write(Tj) conflict: %d-->%d. transaction#%d needs to be forwards adjusted\n",trans_id,Tj->transid,Tj->transid);
                            break;
                        }
                    }
                    if (status == 1)
                        break;
                }
            }

            //phase 4 - committing transaction and backward adjusting the conflicted transactions
            if (status == -1){
                if (present.sot == INT_MAX){
                    present.sot = clock();
                }
                
                for (auto x : livetrans){
                    if (find(BTlist.begin(), BTlist.end(),x->transid) != BTlist.end()){
                        x->sot = present.sot - 1;
                    }
                }

                for (auto x : present.readset){
                    dataitems[x].rts = present.sot;
                }

                for (auto x : localdata ){      //make the changes gloabal
                    dataitems[x.ind].data = x.val;  //commit
                    dataitems[x.ind].wts = present.sot;
                }
                printf("%ld: Thread %d committed transaction #%d\n",clock(), thread_id, trans_id); 
                committed++;    
            }     
        }
        
        //remove the present transaction from active transactions
        livetrans.erase(remove_if(livetrans.begin(), livetrans.end(), [&](Ti* const & present){
            return present->transid == trans_id;
            }), 
            livetrans.end());

        pthread_rwlock_unlock(&rwlock);   //unlock - critical section ends
        
        return;
    }

    void testTrans(int thread_id){
        vector<change_data> localdata;   //threads writes to this locally
        localdata.resize(datasize);
        struct change_data one_op;
        struct Ti temp;
        // vector<int> readset;             //dataitems transactions reads
        // vector<int> writeset;           //dataitems transactions writes
        // long int start_time; 

        int trans_id = thread_id;
        while(trans_id<num_trans) {
        
            localdata.clear();          //clear the local variables for next transaction
            temp.readset.clear();
            temp.writeset.clear();
            temp.tr.clear();

            for (auto x : transactions[trans_id]) {  //for operations in a transaction
                switch (x.operation) {
                    case 'b': { //begin
                        temp.transid = trans_id;
                        temp.sot = INT_MAX; //to compare during validation
                        addme_tolive(&temp);
                        printf("%ld: Thread %d beginning transaction %d\n",clock(), thread_id, trans_id);
                        break;
                    }
                    case 'r': { //read
                        read_data (trans_id, x.dataitem, &temp, thread_id);
                        break;
                    }
                    case 'w': { //write
                        some_lock.lock();
                        one_op.val = rand() % 10 + 1;
                        one_op.ind = x.dataitem;
                        localdata.push_back(one_op);  //writes a random value to local data
                        temp.writeset.push_back(x.dataitem); //adds to writeset
                        printf("%ld: Writing %d for dataitem:%d for transaction %d invoked by thread %d\n",clock(), one_op.val, x.dataitem, trans_id,thread_id);
                        some_lock.unlock();
                        break;
                    }
                    case 'd': { //delay
                        usleep((rand() % 20 + 1) * 1000);
                        break;
                    }
                    case 'c': { //try-commit
                        // sort(readset.begin(), readset.end());  //sort to find the set intersection easily
                        // sort(writeset.begin(), writeset.end());
                        printf("%d is about to enter validation\n",trans_id);
                        valwrite (trans_id, thread_id, localdata, temp);
                        break;
                    }
                }  //end of switch 
            } //end of for loop
            trans_id += num_threads;  
        } //end of while loop
        return;
    }

}; //end of class OCC_DA


int main(){
    
    fstream file;
    string filename,line, str1;
    filename = "inp-params.txt";  //name of input file
    file.open(filename.c_str());
  
    file >> num_threads >> num_trans; //reading from file
    getline(file, line);

    transactions.resize(num_trans);

    struct operations temp;
    temp.dataitem = 0;
    int trans_counter = 0;
    while (trans_counter < num_trans){ //reading all transactions from file
        getline(file, line);
        istringstream iss(line);  //breaking the line at space
        vector<string> tokens{istream_iterator<string>{iss},
                      istream_iterator<string>{}};
        for(auto str1: tokens){     //preparing the transaction format 
            //cout<<str1<< "  ";
            if(str1[0] == 'r' || str1[0] == 'w'){
                temp.operation = str1[0];
                temp.dataitem = int(str1[1])-48;
            }
            else
                temp.operation = str1[0];
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

    OCC_DA *sched = new OCC_DA();  //constructor

    //thread garbage_tid = thread( &OCC_DA::remove_garbage, sched)
    thread thd[num_threads];
    for (int i = 0; i < num_threads; i++)   //allocating threads
        thd[i] = thread( &OCC_DA::testTrans, sched,i);

    for (int i = 0; i < num_threads; i++)       //waiting for join
        thd[i].join();

    printf("# committed transactions : %d\n", committed);
    // printf("Order:\n");
    // for(auto x : order)
    //     printf("%d-%ld\n",x.transaction_id,x.sot);

    return 0; 
}  //end of the program