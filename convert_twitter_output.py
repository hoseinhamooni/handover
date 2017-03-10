import json
import sys

def find_tuples(input_file, output_file):
	f_out = open(output_file ,'w')	
	f_in = open(input_file,'r')
	for line in f_in:
		if(len(line) > 1000):
			x = json.loads(line)
			times = x['timestamp_ms']
			screen_name = (x['user']['screen_name'])
			user_id =  str(x['user']['id'])
			f_out.write(times + ',' + screen_name + ',' + user_id +'\n')
			
if __name__ == "__main__":
	input_file = sys.argv[1]
	output_file = sys.argv[2]
	find_tuples(input
