# cleaning CSV file
import csv

with open('russell-3000.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    rows = list(reader)

empty_row_indicies = [i for i in range(len(rows)) if (len(rows[i]) == 0 or '\xa0' in rows[i])]

print('Empty rows:', empty_row_indicies)

start = empty_row_indicies[0] + 1
end = empty_row_indicies[1]
cleaned_rows = rows[start:end]

with open('russell-3000-clean.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerows(cleaned_rows)
