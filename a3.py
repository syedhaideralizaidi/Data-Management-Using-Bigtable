from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters

client = bigtable.Client(project='<project-id>', admin=True)
instance = client.instance('<instance-id>')
table_id = 'sensors'
table = instance.table(table_id)

cf1 = table.column_family('co')
cf2 = table.column_family('humidity')
cf3 = table.column_family('light')
cf4 = table.column_family('lpg')
cf5 = table.column_family('motion')
cf6 = table.column_family('smoke')
cf7 = table.column_family('temp')

table.create(column_families=[cf1, cf2, cf3, cf4, cf5, cf6, cf7])

row_key = 'device1_1620881420'  # sample row key
timestamp_micros = 1620881420000000  # sample timestamp in microseconds

mutation = table.mutation()
mutation.set_cell(column_family_id='temp', column='reading', value='75.0', timestamp_micros=timestamp_micros)
mutation.set_cell(column_family_id='humidity', column='reading', value='45.0', timestamp_micros=timestamp_micros)
mutation.set_cell(column_family_id='co', column='reading', value='0.01', timestamp_micros=timestamp_micros)
mutation.set_cell(column_family_id='smoke', column='reading', value='0.001', timestamp_micros=timestamp_micros)
mutation.set_cell(column_family_id='lpg', column='reading', value='0.02', timestamp_micros=timestamp_micros)
mutation.set_cell(column_family_id='motion', column='reading', value='false', timestamp_micros=timestamp_micros)
mutation.set_cell(column_family_id='light', column='reading', value='true', timestamp_micros=timestamp_micros)

table.mutate_rows({row_key: mutation})

# # Query 1: Average reading of a particular sensor (e.g., temperature, humidity etc.) within a certain time range
# start_ts_micros = 1620880000000000  # start timestamp in microseconds
# end_ts_micros = 1620885000000000  # end timestamp in microseconds
# column_family_id = 'temp'  # sample column family

# # Create a filter for the time range and column family
# filter_ = row_filters.TimestampRangeFilter(start_ts_micros, end_ts_micros)
# filter_.chain(row_filters.ColumnFamilyFilter(column_family_id))

# # Query the table
# rows = table.read_rows(filter_=filter_)
# readings = []
# for row in rows:
#     # Extract the sensor reading value from the row
#     cells = row.cells[column_family_id]['reading']
#     for cell in cells:
#         readings.append(float(cell.value))

# # Calculate the average reading
# avg_reading = sum(readings) / len(readings)
# print(f"Average reading of {column_family_id} between {start_ts_micros} and {end_ts_micros}: {avg_reading}")

# # Query 2: Average reading of a particular sensor (e.g., temperature, humidity etc.) by a certain
