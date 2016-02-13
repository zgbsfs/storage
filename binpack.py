''' Partition a list into sublists whose sums don't exceed a maximum 
    using a First Fit Decreasing algorithm. See
    http://www.ams.org/new-in-math/cover/bins1.html
    for a simple description of the method.
'''

    
class Bin(object):
    ''' Container for items that keeps a running sum '''
    def __init__(self):
        self.items = []
        self.sum = 0
    
    def append(self, item):
        self.items.append(item)
        self.sum += item[1]



    def __str__(self):
        ''' Printable representation '''
        return 'Bin(sum=%d, items=%s)' % (self.sum, str(self.items))
        
        
def pack(values, maxValue):
    values = sorted(values, key=lambda x: x[1], reverse=True)
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


if __name__ == '__main__':
    import random

    def packAndShow(aList, maxValue):
        ''' Pack a list into bins and show the result '''
        print 'List with sum', sum(aList), 'requires at least', (sum(aList)+maxValue-1)/maxValue, 'bins'
        
        bins = pack(aList, maxValue)
        
        print 'Solution using', len(bins), 'bins:'
        for bin in bins:
            print bin
        
        print
        
        
    aList = [10,9,8,7,6,5,4,3,2,1]
    packAndShow(aList, 11)
    
    aList = [ random.randint(1, 11) for i in range(100) ]
    packAndShow(aList, 11)
    
