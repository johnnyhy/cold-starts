#!/usr/bin/env python2.7

from factors import *
from csv import DictReader, DictWriter


def main():
    filename = ""
    outfilename = filename + ".out"
    outfile = open(outfilename, 'w')
    writer = DictWriter(outfile)
    with open(filename, 'r') as csvfile:
        reader = DictReader(csvfile)
        for row in reader:
            # estimate and append cold starts to trace file
            cold_start = cold_start_from_memory(row["language"], row["memory"])
            row["cold_start_memory"] = cold_start
            writer.writerow(row)


if __name__ == "__main__":
    main()
