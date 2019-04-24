from pyspark import SparkContext
sc = SparkContext(master = 'local[4]')

distFile = sc.textFile("data.txt")

nr_bins = 10
v_min = 0.0
v_max = 1.0
binwidth = (v_max - v_min) / nr_bins



def bin_counts(value):

        i, s, v = value.split('\t')
        secondary = int(s)
        value = float(v)

        if value < v_min:
            bin = 0
        elif value > v_max:
            bin = nr_bins - 1
        else:
            bin = int((value - v_min) / binwidth)
        return bin, 1


counts = distFile.map(lambda t: bin_counts(t)) \
	.reduceByKey(lambda a, b: a + b) 

cc = counts.collect()
print (cc)

