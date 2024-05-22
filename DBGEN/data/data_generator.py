"""
set up data stream

# TODO: specify rules here
"""

# load packages
import os
import csv
import random

# reproducibility
random.seed(0)

# query 5
'''
dataset_files = ["lineitem.tbl", "orders.tbl",
                 "supplier.tbl", "customer.tbl", "nation.tbl","region.tbl"]

# filtered 
FILTERED_TBL_SET = set(["region.tbl", "orders.tbl"])
'''
# query 7
dataset_files = ["lineitem.tbl", "orders.tbl",
                 "supplier.tbl", "customer.tbl", "nation.tbl"]

# filtered 
FILTERED_TBL_SET = set(["nation.tbl", "supplier.tbl", "customer.tbl"])

# target dump path
csv_file_path = "source_data.csv"


def data_generator():
    dataset_firstHalf = []
    dataset_lastHalf = {table: [] for table in dataset_files}
    final_dataset = []
    add_count = 0
    delete_count = 0
    total_size = 0

    # ====================================
    # ========== load ====================
    # ====================================

    for file_name in dataset_files:
        with open(file_name, "r") as file:
            file_data = file.readlines()

        n = len(file_data)
        total_size += n
        print(f"{file_name} size is {n}")
        # if not nation or region, split data into two half
        if file_name not in FILTERED_TBL_SET:
            file_data1 = file_data[:n // 2]
            file_data2 = file_data[n // 2:]

            for line in file_data1:
                dataset_firstHalf.append(
                    f'+|{file_name}|{line.strip()}'.replace(',', '.'))
                # log to final dataset
                final_dataset.append(
                    f'+|{file_name}|{line.strip()}'.replace(',', '.'))
                add_count += 1

            for line in file_data2:
                dataset_lastHalf[file_name].append(
                    f'+|{file_name}|{line.strip()}'.replace(',', '.'))
        else:
            # log to final dataset
            for line in file_data:
                final_dataset.append(
                    f'+|{file_name}|{line.strip()}'.replace(',', '.'))
                add_count += 1

    # ===========================================================
    # ========== randomize ======================================
    # ===========================================================
    # perform random delete one tuple but must not be nation.tbl and 
    # region.tbl and insert the next tuple from the corresponding dataset.
    
    random.shuffle(dataset_firstHalf)
    while dataset_firstHalf:
        # random shuffle the dataset, choose one tuple to delete
        # * shuffle should be done outside of while loop, due to symmetries in randomness
        # random.shuffle(dataset_firstHalf) 
        deleted_tuple = dataset_firstHalf.pop()
        table_name = deleted_tuple.split("|")[1]

        # if from FILTERED_TBL_SET, choose another a new tuple
        while table_name in FILTERED_TBL_SET or not dataset_lastHalf[table_name]:
            deleted_tuple = dataset_firstHalf.pop()
            table_name = deleted_tuple.split("|")[1]

        # log to final dataset
        final_dataset.append(f'-{deleted_tuple[1:]}'.strip('"'))
        delete_count += 1

        # choose a tuple from last half dataset to insert
        inserted_tuple = dataset_lastHalf[table_name].pop()

        # log to final dataset
        final_dataset.append(inserted_tuple.strip('"'))
        add_count += 1

    # if odd data, have one more insert left
    for table_name, table_data in dataset_lastHalf.items():
        while table_data:
            final_dataset.append(table_data.pop())
            add_count += 1

    print(f'add count: {add_count}')
    print(f'delete count: {delete_count}')
    print(f'total size: {total_size}')

    # ====================================
    # ========== dump to file ============
    # ====================================

    # Write the final dataset to a CSV file
    # with open(csv_file_path, 'w', newline='') as file:
    #     writer = csv.writer(file)
    #     for row in final_dataset:
    #         writer.writerow([row])
    with open(csv_file_path, 'w') as f:
        f.writelines("\n".join(final_dataset))
    
    print('CSV file created and data written successfully.')


if __name__ == '__main__':
    data_generator()
