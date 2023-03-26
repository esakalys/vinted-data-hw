
import os
import concurrent.futures
import functions as f

class MapReduce():

    def __init__(self, map, reduce, output):
        self.map = map
        self.reduce = reduce
        self.output = output

    def run(self):

        # MAPPING

        # List to store final mapping results
        map_results = []
        # Iterating through all map functions
        for key in self.map.keys():
            # Input dataset for the map function
            input = key
            # Map function itself
            map_func = self.map[key]
            # NOTE: assuming partitioning is already carried out
            # List with the partition file names
            partitions = [part for part in os.listdir(input)]
            # Initiating a thread pool executor to allow for concurrent execution
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # List for threads that will carry out the map tasks
                mappers = []
                # Submiting the read tasks to the executor for each partition
                readers = [
                    executor.submit(f.read_ds, f'{input}/{part}')
                    for part in partitions
                    ]
                # As readers complete their tasks, submit their results to be processed by the mappers
                for reader in concurrent.futures.as_completed(readers):
                    mappers.append(executor.submit(map_func, reader.result()))
                # As the mappers complete their tasks, store results in final result list
                for mapper in concurrent.futures.as_completed(mappers):
                    map_results.append(mapper.result())
        
        # SORTING

        # Merging the mapper outputs
        map_results = f.flatten(map_results)
        # Sorting based on key
        map_results = sorted(map_results, key=lambda x: x['key']) 

        # Creating reducer partitions for each key
        partitions = []
        key = None
        for result in map_results:
            if key != result['key']:
                partitions.append(
                    {
                        'key': result['key'],
                        'values': [result['value']]
                    }
                )
                key = result['key']
            else:
                partitions[-1]['values'].append(result['value'])

        # REDUCING

        reduce_func = self.reduce
        reduce_results = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Initiating a reducer for each reduce partition
            reducers = [
                executor.submit(reduce_func, part)
                for part in partitions
            ]
            # Storing results of each reduce job as it completes
            for reducer in concurrent.futures.as_completed(reducers):
                reduce_results.append(reducer.result())

        # Saving the result
        self.result = f.flatten(reduce_results)
        f.write_ds(self.result, self.output)
