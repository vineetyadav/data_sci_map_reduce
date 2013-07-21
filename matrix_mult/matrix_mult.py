import MapReduce
import sys
import collections
"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    
    key = record[0]
    value = record[1]
    #words = value.split()
    #for w in words:
    if record[0]=='a':
        for i in range(0,5):
            mr.emit_intermediate((record[1],i), (record[0],record[2],record[3]))
    elif record[0]=='b':
        for i in range(0,5):
            mr.emit_intermediate((i,record[2]), (record[0],record[1],record[3]))
    
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    #print key,  list_of_values
    total = 0
    final_a = {}
    final_b = {}
    
    for each in range(0,5):
        final_a.setdefault(each,0)
    for  each in range(0,5):
        final_b.setdefault(each,0)
    #print final_a, final_b
    for v in list_of_values:
      if v[0] =='a':
          
          final_a[v[1]] = v[2]
    
      elif v[0]=="b":
          final_b[v[1]] = v[2]
    #final_a_ord = [e[1] for e in collections.OrderedDict(sorted(final_a.items()))]
    #final_b_ord = [e[1] for e in collections.OrderedDict(sorted(final_b.items()))]
    final_a_ord = [final_a[e] for e in sorted(final_a)]
    final_b_ord = [final_b[e] for e in sorted(final_b)]
    
    sum1 = 0
    for each1 in range(0,len(final_a_ord)):
        sum1 = sum1 + (final_a_ord[each1]*final_b_ord[each1])
    
    if sum1:
        mr.emit((key[0],key[1],sum1))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
