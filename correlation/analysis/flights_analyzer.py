import csv
import matplotlib.pyplot as plt

if __name__ == "__main__":
  print("hello world")

  elapsed_times = []
  air_times = []
  distances = []

  with open('/home/yingjun/Downloads/flights.csv') as csvfile:
    rows = csv.reader(csvfile, delimiter = ',')

    count = 0
    for row in rows:
      # print(row)
      # print(len(row))
      if count > 0:
        if row[15] != '' and row[16] != '' and row[17] != '':
          elapsed_times.append(float(row[15]))
          air_times.append(float(row[16]))
          distances.append(float(row[17]))
      if count > 1000:
        break
      count += 1

  # print(air_times)
  # print(distances)

  plt.plot(distances, air_times, 'ro')
  plt.plot(distances, elapsed_times, 'bo')
  plt.show()
