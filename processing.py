#!/usr/bin/env python2.7

from factors import *
from csv import DictReader, DictWriter


def main():
    filename = ""
    outfilename = filename + ".out"
    with open(filename, 'r') as csvfile:
        reader = DictReader(csvfile)
        for row in reader:
            # get cold start time
            # append cold start time to line
            print("nothing")


if __name__ == "__main__":
    main()
