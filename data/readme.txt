This is the small dataset.

filelist.txt is the list of the files that should be included in the processing
file0.txt - file5.txt are the text files to process

You should use 3 processes for this example.

The hash you should use for this example is simply adding the ascii values up for all
the characters in the word.  For example, "cat" is 99 + 97 + 116 = 312.  This number
should of course be moded by the number of processes.  So 312 % 3 = 0, meaning "cat"
would go to process 0.


