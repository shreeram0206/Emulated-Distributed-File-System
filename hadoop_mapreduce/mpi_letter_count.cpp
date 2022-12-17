#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <mpi.h>
#include <unordered_map>
#include <stddef.h>
#include <cstring>
#include <iostream>

using namespace std;

constexpr int WORD_MAX = 10;

struct CHAR_PAIR {
  int count;
  char c;
};

int  main(int argc, char **argv)
{
    int size;
    int rank;
    int tag;
    int chunksize;
    int letters[26];
    int start;
    int charCount = 0;
    int result[26];
    int Addletters[26];
    int c;
    char *full_char_array;

    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    cout << "Starting MPI Rank: " << rank << endl;

    MPI_Datatype CHAR_PAIR_type;
    MPI_Datatype CHAR_PAIR_types[2] = {MPI_INT, MPI_CHAR};
    int CHAR_PAIR_blocklen[2] = {1, 1};
    MPI_Aint CHAR_PAIR_disp[2] = {offsetof(CHAR_PAIR, count), offsetof(CHAR_PAIR,c)};
    MPI_Type_create_struct(2, CHAR_PAIR_blocklen, CHAR_PAIR_disp, CHAR_PAIR_types, &CHAR_PAIR_type);
    MPI_Type_commit(&CHAR_PAIR_type);


    if (rank==0){
            FILE *file;
            cout << "Opening File" << endl;
            file = fopen("gutenberg-1G.txt", "r");
            cout << "Opened file2" << endl;
            while ((c = getc(file)) != EOF){
                charCount++;
            }

            cout << "Counted " << charCount << " chars" << endl;

            fclose(file);
            full_char_array = (char*)malloc(sizeof(char) * charCount);

            file = fopen("gutenberg-1G.txt", "r");
            int char_index = 0;
            while ((c = getc(file)) != EOF){

              if (c > 31 && c < 128) {
                  full_char_array[char_index] = c;
                  char_index++;
            }
         }
         fclose(file);
         cout << "Word array populated" << endl;

    }
    //Broadcast wordCount to all nodes
    MPI_Bcast(&charCount, 1, MPI_INT, 0, MPI_COMM_WORLD);

    // Create a buffer that will hold a subset of the words
    int elements_per_proc = charCount/size;
    char *sub_char_array = (char*)malloc(sizeof(char) * elements_per_proc);

    cout << "Scatter to processes Rank: " << rank << endl;
    // Scatter the random numbers to all processes
    MPI_Scatter(full_char_array, elements_per_proc, MPI_CHAR, sub_char_array,
            elements_per_proc, MPI_CHAR, 0, MPI_COMM_WORLD);
    cout << "Scatter done " << endl;

    delete [] full_char_array;
    for (int i = 0; i < 10; i++) {
        cout << "Rank: " << rank << " " << sub_char_array[i] << endl;
    }
    unordered_map<char, int> char_map;
    for (int i = 0; i < elements_per_proc; i++) {
      if (rank==0 && i % 100000 == 0) {
        cout << ".";
      }
      char_map[sub_char_array[i]] ++;
    }

    CHAR_PAIR *char_pair_array = (CHAR_PAIR*)malloc(sizeof(CHAR_PAIR) * elements_per_proc);

    unordered_map<char, int>::iterator it;

    int i = 0;
    for (it = char_map.begin(); it != char_map.end(); it++) {
      if (rank==0) {
        cout << "Pair Key: " << it->first << " value: " << it->second << endl;
      }
      char_pair_array[i].count = it->second;
      char_pair_array[i].c = it->first;
      i++;
    }



    CHAR_PAIR *final_char_pair_array = (CHAR_PAIR*)malloc(sizeof(CHAR_PAIR) * 200);

    cout << "Gather from processes" << endl;
    MPI_Gather(char_pair_array, 200, CHAR_PAIR_type, final_char_pair_array, 200, CHAR_PAIR_type, 0, MPI_COMM_WORLD);



if (rank==0) {
    for(int j = 0; j < 200; j++) {
        cout << "Pair word : " << char_pair_array[j].c << " count: " << char_pair_array[j].count << endl;
    }
    // Reduce final_word_pair_array to a final hashmap
    cout << "Final Reduce" << endl;
    unordered_map<char, int> final_char_map;
    for (int i = 0; i < 97; i++) {
        if (char_pair_array[i].c != 0 && char_pair_array[i].count > 0){
          final_char_map[char_pair_array[i].c] += char_pair_array[i].count;
        }
    }

    int minval = 1000;
    int maxval = 0;

    for (it = final_char_map.begin(); it != final_char_map.end(); it++) {
        cout << "Key: " << it->first << " Value: " << it->second << endl;
        if (it->second < minval) {
          minval = it->second;
        }

        if (it->second > maxval) {
          cout << "High value: " << it->first << endl;
          maxval = it->second;
        }

    }

    cout << "Least common key: " << minval << " most common key: " << maxval << endl;
    cout << "Final word map" << endl;
    i = 0;
    for (it = final_char_map.begin(); it != final_char_map.end(); it++) {
        if (i == 10)
                break;
        cout << "Pair Key: " << it->first << " value: " << it->second << endl;
        i++;
    }



    cout << "Most Frequent Chars: ";
    for (it = final_char_map.begin(); it != final_char_map.end(); it++) {
      if (it->second == maxval) {
        cout << it->first << " "  << it->second << " ";
      }
    }

    cout << "Least Frequent Chars: ";
    for (it = final_char_map.begin(); it != final_char_map.end(); it++) {
      if (it->second == minval) {
        cout << it->first << " " << it->second << " ";
      }
    }
}
    MPI_Finalize();
    return 0;
}