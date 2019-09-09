import sys
from random import randint

num_threads, num_trans = map(int, input().split())
SIZE = 10
sys.stdout = open('inp-params.txt', 'w')
print (num_threads, num_trans)
for i in range (num_trans):
	temp_trans = "b "
	total_op = randint (4, 6);
	num_reads = (int)(0.8 * total_op)
	if (num_reads != 0):
		temp_trans += "d "
	for j in range (num_reads):
		data_item = randint (0, SIZE - 1)
		temp_trans += "r" + str(data_item) + " "
	num_writes = total_op - num_reads
	if (num_writes != 0):
		temp_trans += "d "
	for j in range (num_writes):
		data_item = randint (0, SIZE - 1)
		temp_trans += "w" + str(data_item) + " "
	temp_trans += "c"
	print (temp_trans)



