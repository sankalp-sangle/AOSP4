import os

file_names = ['testdata_1.txt', 'testdata_2.txt', 'testdata_3.txt']

DIR = "bin/input"

count_of_words = {}


for files in file_names:
    with open(DIR + '/' + files, 'r') as f:
        for line in f:
            line = line.replace('.', ' ')
            line = line.replace('  ', ' ')

            for word in line.split():
                if word in count_of_words:
                    count_of_words[word] += 1
                else:
                    count_of_words[word] = 1

# sort count_of_words according to key
sorted_count_of_words = sorted(count_of_words.items(), key=lambda x: x[1], reverse=True)

for key, value in sorted_count_of_words:
    # dump key and value to a file truth.txt
    with open('truth.txt', 'a') as f:
        f.write(key + ' ' + str(value) + '\n')

OUTPUT_DIR = "bin/output/"
# Get list of filenames in output directory
output_files = os.listdir(OUTPUT_DIR)
for file in output_files:
    with open(OUTPUT_DIR + file, 'r') as f:
        # Split line of file into key and value
        for line in f:
            key, value = line.split()
            if key in count_of_words:
                # Check if value is equal to count_of_words
                if int(value) == count_of_words[key]:
                    # print("PASS: " + key + " " + value)
                    # delete key from count_of_words
                    del count_of_words[key]
                    pass
                else:
                    print("FAIL: " + key + " " + value + " Actual: " + str(count_of_words[key]))
            else:
                print("FAIL: " + key + " " + value)

print(len(count_of_words))
print(count_of_words)