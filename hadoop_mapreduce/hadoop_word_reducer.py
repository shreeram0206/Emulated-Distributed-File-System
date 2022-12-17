#!/usr/bin/env python
"""A more advanced Reducer, using Python iterators and generators."""
from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file, separator='\t'):
        for line in file:
                yield line.rstrip().split(separator, 1)

def main(separator='\t'):
        # input comes from STDIN (standard input)
        data = read_mapper_output(sys.stdin, separator=separator)
        most_frequent = []
        most_frequent_cnt = 1
        least_frequent = []
        # groupby groups multiple word-count pairs by word,
        # and creates an iterator that returns consecutive keys and their group:
        # current_word - string containing a word (the key)
        # group - iterator yielding all ["&lt;current_word&gt;", "&lt;count&gt;"] items
        for current_word, group in groupby(data, itemgetter(0)):
                try:
                        total_count = sum(int(count) for current_word, count in group)
                        if total_count > most_frequent_cnt:
                                most_frequent_cnt = total_count
                                del most_frequent[:]
                                most_frequent.append(current_word)
                        elif total_count == most_frequent_cnt:
                                most_frequent.append(current_word)
                        if total_count == 1:
                                least_frequent.append(current_word)
                        print "%s%s%d" % (current_word, separator, total_count)
                except ValueError:
                        # count was not a number, so silently discard this item
                        pass
        print("\nMost Frequent Word:")
        for word in most_frequent:
                print(word)
        print("\nLeast Frequent Words:")
        for word in least_frequent:
                print(word)

if __name__ == "__main__":
        main()