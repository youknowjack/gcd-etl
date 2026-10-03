import pyarrow.dataset as ds
import json
import sys

dataset = ds.dataset(sys.argv[1], format="parquet")
output_prefix = sys.argv[2]

#print(dataset.schema.to_string(show_field_metadata=False))

scanner = dataset.scanner()
#print(scanner.count_rows())

for index, batch in enumerate(scanner.to_batches()):
  out_filename = f'{output_prefix}.{index}.json'
  out = open(out_filename, "w")
  out.write(batch.to_pandas().to_json(orient='records', lines=True))
  out.close()
  print(f'Wrote to {out_filename}')
