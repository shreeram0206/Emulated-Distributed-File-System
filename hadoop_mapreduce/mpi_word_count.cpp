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

struct WORD {
  char w[WORD_MAX];
  bool operator==(const WORD & word) const {
      for (int i = 0; i < WORD_MAX; i++) {

          if (w[i] != word.w[i]) {
              return false;
          }
      }
      return true;
  }
};

struct WORD_PAIR {
  int count;
  char w[WORD_MAX];
};

class WordHashFunction {
    public:

        size_t operator() (const WORD& word) const {
            return word.w[0] + word.w[1] + word.w[2] + word.w[3];
        }
};

int  main(int argc, char **argv)
{
    int size;
    int rank;
    int tag;
    int chunksize;
    int letters[26];
    int start;
    int wordCount = 0;
    int result[26];
    int Addletters[26];
    int c;
    WORD *full_word_array;

    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    cout << "Starting MPI Rank: " << rank << endl;
    MPI_Datatype WORD_type;
    MPI_Datatype WORD_types[1] = {MPI_CHAR};
    int WORD_blocklen[1] = {WORD_MAX};
    MPI_Aint WORD_disp[1] = {offsetof(WORD, w)};
    MPI_Type_create_struct(1, WORD_blocklen, WORD_disp, WORD_types, &WORD_type);
    MPI_Type_commit(&WORD_type);


    MPI_Datatype WORD_PAIR_type;
    MPI_Datatype WORD_PAIR_types[2] = {MPI_INT, MPI_CHAR};
    int WORD_PAIR_blocklen[2] = {1, WORD_MAX};
    MPI_Aint WORD_PAIR_disp[2] = {offsetof(WORD_PAIR, count), offsetof(WORD_PAIR,w)};
    MPI_Type_create_struct(2, WORD_PAIR_blocklen, WORD_PAIR_disp, WORD_PAIR_types, &WORD_PAIR_type);
    MPI_Type_commit(&WORD_PAIR_type);


    if (rank==0){
            FILE *file;
            cout << "Opening File" << endl;
            file = fopen("gutenberg-1G.txt", "r");
            cout << "Opened file2" << endl;
            while ((c = getc(file)) != EOF){
              if (c == ' ' || c == '\n' || c == '\t' ) {
                //New word
                wordCount++;
//                cout << "New Word" << endl;
              }
            }

            cout << "Counted " << wordCount << " words" << endl;

            fclose(file);
            full_word_array = (WORD*)malloc(sizeof(WORD) * wordCount);

            file = fopen("gutenberg-med.txt", "r");
            int word_index = 0;
            int char_index = 0;
            while ((c = getc(file)) != EOF){
              if (c == ' ' || c == '\n' || c == '\t' ) {
                //New word
                word_index++;
                char_index = 0;
                continue;
              }

              if (c > 31 && c < 128) {
                  full_word_array[word_index].w[char_index] = c;
                  char_index++;
            }
         }
         fclose(file);
         cout << "Word array populated" << endl;

    }
    //Broadcast wordCount to all nodes
    MPI_Bcast(&wordCount, 1, MPI_INT, 0, MPI_COMM_WORLD);

    // Create a buffer that will hold a subset of the words
    int elements_per_proc = wordCount/size;
    WORD *sub_word_array = (WORD*)malloc(sizeof(WORD) * elements_per_proc);

    cout << "Scatter to processes Rank: " << rank << endl;
    // Scatter the random numbers to all processes
    MPI_Scatter(full_word_array, elements_per_proc, WORD_type, sub_word_array,
            elements_per_proc, WORD_type, 0, MPI_COMM_WORLD);
    cout << "Scatter done " << endl;

    delete [] full_word_array;
    for (int i = 0; i < 10; i++) {
        cout << "Rank: " << rank << " " << sub_word_array[i].w << endl;
    }
    unordered_map<WORD, int, WordHashFunction> word_map;
    for (int i = 0; i < elements_per_proc; i++) {
      if (rank==0 && i % 100000 == 0) {
        cout << ".";
      }
      word_map[sub_word_array[i]] ++;
    }

    WORD_PAIR *word_pair_array = (WORD_PAIR*)malloc(sizeof(WORD_PAIR) * elements_per_proc);

    unordered_map<WORD, int, WordHashFunction>::iterator it;

    int i = 0;
    for (it = word_map.begin(); it != word_map.end(); it++) {
      if (rank==0) {
        cout << "Pair Key: " << it->first.w << " value: " << it->second << endl;
      }
      word_pair_array[i].count = it->second;
      for (int j = 0; j < WORD_MAX; j++) {
        word_pair_array[i].w[j] = it->first.w[j];
      }
      i++;
    }



    WORD_PAIR *final_word_pair_array = (WORD_PAIR*)malloc(sizeof(WORD_PAIR) * wordCount);

    cout << "Gather from processes" << endl;
    MPI_Gather(word_pair_array, elements_per_proc, WORD_PAIR_type, final_word_pair_array, elements_per_proc, WORD_PAIR_type, 0, MPI_COMM_WORLD);

    for(int j = 0; j < 10; j++) {
        cout << "Pair word : " << word_pair_array[j].w << " count: " << word_pair_array[j].count << endl;
    }


if (rank==0) {
    // Reduce final_word_pair_array to a final hashmap
    cout << "Final Reduce" << endl;
    unordered_map<WORD, int, WordHashFunction> final_word_map;
    for (int i = 0; i < elements_per_proc; i++) {
      struct WORD temp_word;
      memcpy(&temp_word.w, &word_pair_array[i].w[0], WORD_MAX); // = {final_word_pair_array[i].w};
//      cout << "Temp word: " << temp_word.w << " count: " << final_word_pair_array[i].count << endl;
        bool validWord = false;
        for (int j = 0; j < WORD_MAX; j++) {
                if (temp_word.w[j] != 0) {
                        validWord = true;
                        break;
                }
        }
        //cout << "Added" << endl;
        if (validWord && word_pair_array[i].count > 0){
                cout << "Added " << temp_word.w << endl;
                final_word_map[temp_word] += word_pair_array[i].count;
        }
    }

    int minval = 1000;
    int maxval = 0;

    for (it = final_word_map.begin(); it != final_word_map.end(); it++) {
        if (it->second < minval) {
          minval = it->second;
        }

        if (it->second > maxval) {
          cout << "High value: " <<it->first.w << endl;
          maxval = it->second;
        }

    }

    cout << "Least common key: " << minval << " most common key: " << maxval << endl;
    cout << "Final word map" << endl;
    i = 0;
    for (it = final_word_map.begin(); it != final_word_map.end(); it++) {
        if (i == 10)
                break;
        cout << "Pair Key: " << it->first.w << " value: " << it->second << endl;
        i++;
    }



    cout << "Most Frequent Words: ";
    for (it = final_word_map.begin(); it != final_word_map.end(); it++) {
      if (it->second == maxval) {
        cout << it->first.w << " "  << it->second << " ";
      }
    }

    cout << "Least Frequent Words: ";
    int limit = 0;
    for (it = final_word_map.begin(); it != final_word_map.end(); it++) {
      // Only print out first 100 shrtest words
      if (limit == 100)
        break;
      if (it->second == minval) {
        cout << it->first.w << " " << it->second << " ";
      }
      limit++;
    }
}
    MPI_Finalize();
    return 0;
}