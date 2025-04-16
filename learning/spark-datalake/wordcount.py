
from collections import defaultdict
import sys

class MRSongCount:
    def mapper(self, _, song):
        yield (song.strip(), 1)

    def reducer(self, key, values):
        yield (key, sum(values))

    def run(self, lines):
        # MAP
        mapped = []
        for i, line in enumerate(lines):
            for pair in self.mapper(i, line):
                mapped.append(pair)

        # SHUFFLE & SORT
        grouped = defaultdict(list)
        for key, value in mapped:
            grouped[key].append(value)

        # REDUCE
        for key, values in grouped.items():
            for result in self.reducer(key, values):
                print(f"{result[0]}\t{result[1]}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python wordcount.py songs.txt")
        sys.exit(1)

    with open(sys.argv[1], "r") as f:
        lines = f.readlines()

    job = MRSongCount()
    job.run(lines)
