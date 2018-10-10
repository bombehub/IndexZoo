import csv
import matplotlib.pyplot as plt
import os
import random

if __name__ == "__main__":
  times = []
  distances = []
  fares = []
  totals = []
  tips = []

  os.chdir('/home/yingjun/Downloads/chicago-taxi-rides-2016')

  filenames = [x for x in os.listdir('.') if x.endswith('csv') and os.path.getsize(x) > 0]
  print(len(filenames))

  # filenames = random.sample(filenames, 10)
  print(filenames)

  count = 0
  for filename in filenames:
    with open(filename) as csvfile:
      
      is_begin = True
      rows = csv.reader(csvfile, delimiter = ',')
      
      for row in rows:

        if is_begin == True:
          is_begin = False
          continue

        if len(row) > 10:
          if row[3] == '' or row[4] == '' or row[9] == '':
            continue
          # if float(row[4]) < 1:
            # print(row[4] + ' ' + row[9])
            # continue
          if float(row[3]) > 1000 or float(row[3]) < 10:
            continue
          if float(row[4]) > 100 or float(row[4]) < 2:
            continue
          times.append(float(row[3]))
          distances.append(float(row[4]))
          fares.append(float(row[9]))
        count += 1
        if count > 50000:
          break
  print count
  plt.plot(distances, fares, 'ro')
  plt.show()