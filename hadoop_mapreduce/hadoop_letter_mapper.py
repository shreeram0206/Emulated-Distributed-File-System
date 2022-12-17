#!/usr/bin/env python
"""A more advanced Mapper, using Python iterators and generators."""
import sys
import re
from collections import defaultdict
def read_input(file):
        pat = re.compile(r'\s+')
        for line in file:
                # split the line into words
                line = re.sub(pat, '', line)
                yield line

def main(separator='\t'):
        # input comes from STDIN (standard input)
        data = read_input(sys.stdin)
        for words in data:
                letter_dict = defaultdict(int)
                # write the results to STDOUT (standard output);
                # what we output here will be the input for the
                # Reduce step, i.e. the input for reducer.py
                #
                # tab-delimited; the trivial word count is 1
                for letter in words:
                        if letter.isalpha():
                                letter_dict[letter.lower()]+=1
                        else:
                                letter_dict[letter]+=1
                for counter in letter_dict.items():
                        print '%s%s%d' % (counter[0], separator, counter[1])

if __name__ == "__main__":
         main()