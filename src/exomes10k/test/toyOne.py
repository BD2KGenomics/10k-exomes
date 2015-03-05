__author__ = 'CJ'
import sys

file = open("{}".format(sys.argv[1]), 'w')
file2 = open("toyOneOutput.txt", 'w')
file2.write("yay")
file.close()
file2.close()