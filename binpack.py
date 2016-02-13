''' Partition a list into sublists whose sums don't exceed a maximum 
    using a First Fit Decreasing algorithm. See
    http://www.ams.org/new-in-math/cover/bins1.html
    for a simple description of the method.
'''
import copy
    
class Bin(object):
    ''' Container for items that keeps a running sum '''
    def __init__(self):
        self.items = []
        self.sum = 0
    def __iter__(self):
	for item in self.items:
	        yield item
    def append(self, item):
        self.items.append(item)
        self.sum += item[1]
    def __len__(self):
	return len(self.items)
    def __str__(self):
        ''' Printable representation '''
        return 'Bin(sum=%d, items=%s)' % (self.sum, str(self.items))
    def getitempath(self):
	pathlist=[]
	sizebinlist=[]
	sizeinbin=0
	for item in self.items:
		pathlist.append(item[0])
		sizeinbin+=item[1]
		sizebinlist.append(sizeinbin)
	return pathlist,sizebinlist
        
def pack(values, maxValue):
    bins = []
    # import ipdb; ipdb.set_trace()
    for item in values:
        # Try to fit item into a bin
        for bin in bins:
            if bin.sum + item[1] <= maxValue:
                #print 'Adding', item[1], 'to', bin
                bin.append(item)
                break
        else:
            # item[1] didn't fit into any bin, start a new bin
            #print 'Making new bin for', item[1]
            bin = Bin()
            bin.append(item)
            bins.append(bin)
    
    return bins

def packAndShow(aList, maxValue):
        ''' Pack a list into bins and show the result '''
	#print str(aList)+str(maxValue)
	sumofsize =0
	for item in aList:
		sumofsize+=item[1]
        print 'List with sum', sumofsize, 'requires at least', (sumofsize+maxValue-1)/maxValue, 'bins'

        #print aList
        bins = pack(aList, maxValue)
	diffarr=[]
	print 'Solution using', len(bins), 'bins:'
	for bin in bins:
		diffarr.append( maxValue-bin.sum)
            	print bin
		lastbin = bin
	print "sum of different    " + str(sum(diffarr))
	bins.pop()
	diffarr.pop()
	copydiffarr = copy.copy(diffarr)
	delta =0

	for howmany in range(len(lastbin)):
		delta+=max(diffarr)
		diffarr.pop()
	print diffarr
	print copydiffarr

	if delta +maxValue> 2*lastbin.sum:
		print "repack bins"
		for item in lastbin:
			binindex = copydiffarr.index(max(copydiffarr))
			copydiffarr[binindex]=0
			bins[binindex].append(item)
		
	sumofsize=0		
        print 'Solution using', len(bins), 'bins:'
        for bin in bins:
            print bin
	    sumofsize +=abs(maxValue-bin.sum)

	print "sum of different   " + str(sumofsize)
   	return bins 
