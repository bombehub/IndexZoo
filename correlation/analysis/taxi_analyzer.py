import csv
import matplotlib.pyplot as plt

if __name__ == "__main__":
  times = []
  distances = []
  fares = []
  totals = []
  tips = []

  with open('/home/yingjun/Downloads/chicago_taxi_trips_2016_01.csv') as csvfile:
    rows = csv.reader(csvfile, delimiter = ',')
    
    count = 0
    for row in rows:
      if count > 1:
        if row[3] == '' or row[4] == '' or row[9] == '':
          continue
        if float(row[4]) < 1:
          # print(row[4] + ' ' + row[9])
          continue
        # if float(row[3]) > 1000:
          # continue
        if float(row[4]) > 50:
          continue
        times.append(float(row[4]))
        distances.append(float(row[9]))
        # fares.append(float(row[4])) 
        # tips.append(float(row[14]))
        # totals.append(float(row[16]))
      # if count > 10000:
        # break
      count += 1
  # print(times)
  # print(distances)
  # print(fares)
  # print(totals)
  print count
  # plt.plot(times, distances, 'ro')
  # plt.plot(distances, times, 'ro')
  # plt.plot(distances, totals, 'bo')
  # plt.show()
  