import csv
import matplotlib.pyplot as plt

if __name__ == "__main__":
  print("hello world")

  distances = []
  fares = []
  totals = []

  with open('/home/yingjun/Downloads/yellow_tripdata_2017-01.csv') as csvfile:
    rows = csv.reader(csvfile, delimiter = ',')
    
    count = 0
    for row in rows:
      if count > 1:
        distances.append(float(row[4]))
        fares.append(float(row[10])) 
        totals.append(float(row[16]))
      if count > 10:
        break
      count += 1

  print(distances)
  print(fares)
  print(totals)

  # plt.plot(distances, fares, 'ro')
  # plt.plot(distances, totals, 'bo')
  # plt.show()