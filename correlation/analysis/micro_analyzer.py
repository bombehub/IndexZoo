import numpy as np
import csv
import matplotlib.pyplot as plt

def graph(formula, x_range, color):
    x = np.array(x_range)
    y = eval(formula)
    plt.plot(x, y, color)

if __name__ == "__main__":
  with open('data.txt') as csvfile:
    rows = csv.reader(csvfile, delimiter = ',')

    xs = []
    ys = []
    for row in rows:
      xs.append(int(row[1]))
      ys.append(int(row[0]))

  plt.plot(xs, ys, 'ro')
  # plt.show()

  with open('index.txt') as csvfile:
    rows = csv.reader(csvfile, delimiter = ',')

    for row in rows:
      if row[4] != 'NA':
        range_lhs = int(row[1])
        range_rhs = int(row[2])
        epsilon = float(row[6])
        slope = float(row[7])
        intercept = float(row[8])
        print('{},{},{},{},{}'.format(range_lhs, range_rhs, epsilon, slope, intercept))
        graph("x*slope+intercept", range(range_lhs, range_rhs), 'b')
        graph("x*slope+intercept-epsilon", range(range_lhs, range_rhs), 'y')
        graph("x*slope+intercept+epsilon", range(range_lhs, range_rhs), 'g')
  plt.show()