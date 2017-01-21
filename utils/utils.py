import matplotlib.pyplot as pl
import numpy as np
import csv
import h5py


def plot_metrics(results, ylabel="", title="", yscale=1):
    pl.figure()
    reader = csv.reader(results.splitlines())
    data = []
    for row in reader:
        data.append(row)
    data = np.array(data)
    pl.plot((data[1:, 0].astype(float) - data[1, 0].astype(float)) * 1e-3, data[1:, 1:].astype(float) * yscale)
    pl.xlabel("time [s]")
    pl.ylabel(ylabel)
    pl.title(title)


def nexus_files_equal(filename_1, filename_2):
    success = True
    # entire small datasets can be loaded into memory
    small_datasets = [
        '/raw_data_1/detector_1_events/event_index'
    ]
    scalar_datasets = [
        '/raw_data_1/detector_1_events/total_counts',
    ]
    # larger datasets will be read and compared in smaller slices
    datasets = [
        '/raw_data_1/detector_1_events/event_id',
        '/raw_data_1/detector_1_events/event_time_offset'
    ]
    with h5py.File(filename_1, 'r') as f_read_1:
        with h5py.File(filename_2, 'r') as f_read_2:
            for dataset in small_datasets:
                data_1 = f_read_1.get(dataset)
                data_2 = f_read_2.get(dataset)
                if not len(data_1) == len(data_2) or not np.allclose(data_1, data_2, atol=0.01):
                    print("FAIL: Files are different in dataset: " + dataset)
                    success = False

            for dataset in scalar_datasets:
                data_1 = f_read_1.get(dataset)
                data_2 = f_read_2.get(dataset),
                if not np.isclose(data_1, data_2, atol=0.01):
                    print("FAIL: Files are different in dataset: " + dataset)
                    success = False

            for dataset in datasets:
                data_1 = f_read_1.get(dataset)
                data_2 = f_read_2.get(dataset)
                n_slices = 10
                slice_size = int(np.floor(len(data_1) / n_slices))
                for n in range(1, n_slices):
                    lower = (n - 1) * slice_size
                    upper = n * slice_size
                    if not len(data_1) == len(data_2) or not np.allclose(data_1[lower:upper], data_2[lower:upper],
                                                                         atol=0.01):
                        print("FAIL: Files are different in dataset: " + dataset)
                        success = False

    if success:
        print("PASS: Input and output file are almost (floats) equal!")
