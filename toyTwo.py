__author__ = 'CJ'
import sys

file = open("{}".format(sys.argv[1]), 'w')
file2 = open("toyTwoOutput.txt", 'w')
file2.write("write2")
file.close()
file2.close()