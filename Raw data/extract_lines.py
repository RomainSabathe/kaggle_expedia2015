from random import random

file = "test.csv"
n_lines_to_grab = 5e4

# Counting the number of lines
n_lines = 0
with open(file, 'r') as input_stream:
    for _ in input_stream.readlines():
        n_lines += 1
print 'There are %s lines in total.\n' %  n_lines

n_lines_extracted = 0
probability = n_lines_to_grab / float(n_lines) # proba that we pick a line
with open(file, 'r') as input_stream:
    with open("short_%s" % file, 'w') as output_stream:
        output_stream.write(input_stream.readline()) # write the headers
        for line in input_stream.readlines():
            if random() < probability:
                output_stream.write(line)
                n_lines_extracted += 1


print "Created short_%s containing %s lines." % (file, n_lines_extracted)

