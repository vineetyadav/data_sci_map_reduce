import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[1]
    value = record
    #words = value.split()
    #for w in value:
    mr.emit_intermediate(record[1], record)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = {'order':[], 'line_item':[]}
    for v in list_of_values:
        #print v
        total[v[0]].append(v)

    
    for each in total['line_item']:
        total_list = total['order'][0]
        total_list = total_list+each
        
        mr.emit(total_list)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
