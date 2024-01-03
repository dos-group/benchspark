def get_all_job_args(data_dir):
    return {
        'GrepDataGeneration':( # <outputDir> <numLines> <lineLength> <keyword> <occurences>
            [data_dir+'GrepData', f'{2e3:.0f}', '100', 'Computer', f'{1e3:.0f}'],
            [data_dir+'GrepData', f'{4e3:.0f}', '100', 'Computer', f'{2e3:.0f}'],
            ),
        'Grep':( # <inputFile>, <keyword> <outputDir>
            [data_dir+'GrepData', 'Computer', data_dir+'GrepOut'],
            ),
        'GroupByCountDataGeneration':( # <inputTextFileURL> <outputDir> <numWords>
            ['https://openbible.com/textfiles/kjv.txt', data_dir+'GroupByCountData', f'{1e3:.0f}'],
            ['file:///home/username/data/kjv.txt', data_dir+'GroupByCountData', f'{1e3:.0f}'],
            ['gs://cloudbucket/kjv.txt', data_dir+'GroupByCountData', f'{1e3:.0f}'],
            ),
        'GroupByCount':( # <inputFile>, <outputDir>
            [data_dir+'GroupByCountData', data_dir+"GroupByCountOut"],
            ),
        'JoinDataGeneration': ( # <numPages> <numUserVisits> <outputDir1> <outputDir2>
            [data_dir+'JoinData1', data_dir+'JoinData2', f'{1e2:.0f}', f'{1e3:.0f}'],
            [data_dir+'JoinData1', data_dir+'JoinData2', f'{2e2:.0f}', f'{2e3:.0f}'],
            ),
        'Join': ( # <inputFile1:csv> <inputFile2:csv> <outputDir>
            [data_dir+'JoinData1', data_dir+'JoinData2', data_dir+'JoinOut'],
            ),
        'KMeansDataGeneration':( # <outputDir> <numClusters> <numSamples> <numDimensions> [<labeled>=false]
            [data_dir+'KMeansData', '10', f'{1e3:.0f}', '100'],
            [data_dir+'KMeansData', '10', f'{2e3:.0f}', '100'],
            ),
        'KMeans':( # <inputFile:libsvm_format> <k> [<numFeatures>]
            [data_dir+'KMeansData', '10', '100'],
            ),
        'LinearRegressionDataGeneration':( # <outputDir> <numPoints> <dimensions> (dense|libsvm)
            [data_dir+'LinearRegressionData', f'{1e3:.0f}', '100', 'libsvm'],
            [data_dir+'LinearRegressionData', f'{2e3:.0f}', '100', 'libsvm'],
            ),
        'LinearRegression':( # <inputFile:libsvm_format> <MaxIter>, [<numFeatures>]
            [data_dir+'LinearRegressionData', '100', '100'],
            ),
        'LogisticRegressionDataGeneration':( # <outputDir> <numPoints> <dimensions>
            [data_dir+'LogisticRegressionData', f'{1e3:.0f}', '100'],
            [data_dir+'LogisticRegressionData', f'{2e3:.0f}', '100'],
            ),
        'LogisticRegression':( # <inputFile:libsvm> <MaxIter>, [<numFeatures>]
            [data_dir+'LogisticRegressionData', '100', '1000'],
            ),
        'SelectWhereOrderByDataGeneration':( # <outputDir> <numUsers>
            [data_dir+'SelectWhereOrderByData', f'{1e3:.0f}'],
            [data_dir+'SelectWhereOrderByData', f'{2e3:.0f}'],
            ),
        'SelectWhereOrderBy':( # <inputFile>, <outputDir>
            [data_dir+'SelectWhereOrderByData', data_dir+"SelectWhereOrderByOut"],
            ),
        'SortDataGeneration':( # <outputDir> <numLines> <lineLength>
            [data_dir+'SortData', f'{1e3:.0f}', '100'],
            [data_dir+'SortData', f'{2e3:.0f}', '100'],
            ),
        'Sort':( # <inputFile>, <outputDir>
            [data_dir+'SortData', data_dir+"SortOut"],
            ),
        'WordCountDataGeneration':( # <inputTextFileURL> <outputDir> <numWords>
            ['https://openbible.com/textfiles/kjv.txt' , data_dir+'WordCountData', f'{1e3:.0f}'],
            ['file:///home/username/data/kjv.txt', data_dir+'WordCountData', f'{1e3:.0f}'],
            ['gs://cloudbucket/kjv.txt', data_dir+'WordCountData', f'{1e3:.0f}'],
            ),
        'WordCount':( # <inputFile>, <outputDir>
            [data_dir+'WordCountData', data_dir+"WordCountOut"],
            ),
    }
