__author__ = 'CJ'
import sys

file = open("{}".format(sys.argv[1]), 'w')
file2 = open("toyThreeOutput.txt", 'w')
file2.write("write3")
file.close()
file2.close()