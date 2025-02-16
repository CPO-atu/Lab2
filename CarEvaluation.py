from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# This regex will capture tokens (alphanumeric, underscore, dot, and hyphen).
DATA_RE = re.compile(r"[\w.-]+")

class MRCarDoors(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_doors,
                   reducer=self.reducer_get_max)
        ]

    def mapper_get_doors(self, _, line):
        # Tokenize the line using the regex.
        data = DATA_RE.findall(line)
        
        # Check if we have all 7 fields (buying, maint, doors, persons, lug_boot, safety, class)
        if len(data) < 7:
            return
        
        # Only process records in the acceptable category.
        if data[-1] == "acc":
            doors = data[2]
            # Convert the doors value to a number:
            # If the value is "5more", we consider it as 5.
            if doors == "5more":
                door_num = 5
            else:
                door_num = int(doors)
            yield ("max doors", door_num)

    def reducer_get_max(self, key, values):
        # Compute the maximum door count among acceptable records.
        yield (key, max(values))

if __name__ == '__main__':
    MRCarDoors.run()
