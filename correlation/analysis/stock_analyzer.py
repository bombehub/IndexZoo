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

  os.chdir('/home/yingjun/Downloads/Data/Stocks/')

  filenames = [x for x in os.listdir('.') if x.endswith('txt') and os.path.getsize(x) > 0]
  print(len(filenames))

  filenames = random.sample(filenames, 10)
  print(filenames)

  # with open('/home/yingjun/Downloads/chicago_taxi_trips_2016_01.csv') as csvfile:
  #   rows = csv.reader(csvfile, delimiter = ',')
    
  #   count = 0
  #   for row in rows:
  #     if count > 1:
  #       if row[3] == '' or row[4] == '' or row[9] == '':
  #         continue
  #       # if float(row[4]) < 1:
  #         # print(row[4] + ' ' + row[9])
  #         # continue
  #       if float(row[3]) > 1000:
  #         continue
  #       if float(row[4]) > 100:
  #         continue
  #       times.append(float(row[3]))
  #       distances.append(float(row[4]))
  #       fares.append(float(row[9]))
  #     if count > 10000:
  #       break
  #     count += 1
  # print count
  # plt.plot(distances, fares, 'ro')
  # plt.show()
  # 