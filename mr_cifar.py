import json
import numpy as np
from mrjob.job import MRJob


class MyMR(MRJob):
    
    def mapper(self, _, line):
        try:
            image_dict = json.loads(line)
            ((image_name, image_data),) = image_dict.items()            
        except:
            assert False, 'something went wrong' # print statements go into mapper output in MRJob
            return

        color_averages = np.array(image_data).mean(axis=(0, 1))
        max_color_channel = np.argmax(color_averages)
        yield (int(max_color_channel), (image_name, color_averages[max_color_channel]))
        # key has to be int, not np.int64
    
    def reducer(self, max_color_channel, max_color_intensities):
        yield max_color_channel, sorted(max_color_intensities, key=lambda tup: (-tup[1], tup[0]))

        
if __name__ == '__main__': 
    MyMR.run()